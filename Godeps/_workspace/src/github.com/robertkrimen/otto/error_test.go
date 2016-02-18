package otto

import (
	"testing"
)

func TestError(t *testing.T) {
	tt(t, func() {
		test, _ := test()

		test(`
            [ Error.prototype.name, Error.prototype.message, Error.prototype.hasOwnProperty("message") ];
        `, "Error,,true")
	})
}

func TestError_instanceof(t *testing.T) {
	tt(t, func() {
		test, _ := test()

		test(`(new TypeError()) instanceof Error`, true)
	})
}

func TestPanicValue(t *testing.T) {
	tt(t, func() {
		test, vm := test()

		vm.Set("abc", func(call FunctionCall) Value {
			value, err := call.Otto.Run(`({ def: 3.14159 })`)
			is(err, nil)
			panic(value)
		})

		test(`
            try {
                abc();
            }
            catch (err) {
                error = err;
            }
            [ error instanceof Error, error.message, error.def ];
        `, "false,,3.14159")
	})
}

func Test_catchPanic(t *testing.T) {
	tt(t, func() {
		vm := New()

		_, err := vm.Run(`
            A syntax error that
            does not define
            var;
                abc;
        `)
		is(err, "!=", nil)

		_, err = vm.Call(`abc.def`, nil)
		is(err, "!=", nil)
	})
}

func TestErrorContext(t *testing.T) {
	tt(t, func() {
		vm := New()

		_, err := vm.Run(`
            undefined();
        `)
		{
			err := err.(*Error)
			is(err.message, "'undefined' is not a function")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:2:13")
		}

		_, err = vm.Run(`
            ({}).abc();
        `)
		{
			err := err.(*Error)
			is(err.message, "'abc' is not a function")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:2:14")
		}

		_, err = vm.Run(`
            ("abc").abc();
        `)
		{
			err := err.(*Error)
			is(err.message, "'abc' is not a function")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:2:14")
		}

		_, err = vm.Run(`
            var ghi = "ghi";
            ghi();
        `)
		{
			err := err.(*Error)
			is(err.message, "'ghi' is not a function")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:3:13")
		}

		_, err = vm.Run(`
            function def() {
                undefined();
            }
            function abc() {
                def();
            }
            abc();
        `)
		{
			err := err.(*Error)
			is(err.message, "'undefined' is not a function")
			is(len(err.trace), 3)
			is(err.trace[0].location(), "def (<anonymous>:3:17)")
			is(err.trace[1].location(), "abc (<anonymous>:6:17)")
			is(err.trace[2].location(), "<anonymous>:8:13")
		}

		_, err = vm.Run(`
            function abc() {
                xyz();
            }
            abc();
        `)
		{
			err := err.(*Error)
			is(err.message, "'xyz' is not defined")
			is(len(err.trace), 2)
			is(err.trace[0].location(), "abc (<anonymous>:3:17)")
			is(err.trace[1].location(), "<anonymous>:5:13")
		}

		_, err = vm.Run(`
            mno + 1;
        `)
		{
			err := err.(*Error)
			is(err.message, "'mno' is not defined")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:2:13")
		}

		_, err = vm.Run(`
            eval("xyz();");
        `)
		{
			err := err.(*Error)
			is(err.message, "'xyz' is not defined")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:1:1")
		}

		_, err = vm.Run(`
            xyzzy = "Nothing happens."
            eval("xyzzy();");
        `)
		{
			err := err.(*Error)
			is(err.message, "'xyzzy' is not a function")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:1:1")
		}

		_, err = vm.Run(`
            throw Error("xyzzy");
        `)
		{
			err := err.(*Error)
			is(err.message, "xyzzy")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:2:19")
		}

		_, err = vm.Run(`
            throw new Error("xyzzy");
        `)
		{
			err := err.(*Error)
			is(err.message, "xyzzy")
			is(len(err.trace), 1)
			is(err.trace[0].location(), "<anonymous>:2:23")
		}

		script1, err := vm.Compile("file1.js",
			`function A() {
				throw new Error("test");
			}

			function C() {
				var o = null;
				o.prop = 1;
			}
		`)
		is(err, nil)

		_, err = vm.Run(script1)
		is(err, nil)

		script2, err := vm.Compile("file2.js",
			`function B() {
				A()
			}
		`)
		is(err, nil)

		_, err = vm.Run(script2)
		is(err, nil)

		script3, err := vm.Compile("file3.js", "B()")
		is(err, nil)

		_, err = vm.Run(script3)
		{
			err := err.(*Error)
			is(err.message, "test")
			is(len(err.trace), 3)
			is(err.trace[0].location(), "A (file1.js:2:15)")
			is(err.trace[1].location(), "B (file2.js:2:5)")
			is(err.trace[2].location(), "file3.js:1:1")
		}

		{
			f, _ := vm.Get("B")
			_, err := f.Call(UndefinedValue())
			err1 := err.(*Error)
			is(err1.message, "test")
			is(len(err1.trace), 2)
			is(err1.trace[0].location(), "A (file1.js:2:15)")
			is(err1.trace[1].location(), "B (file2.js:2:5)")
		}

		{
			f, _ := vm.Get("C")
			_, err := f.Call(UndefinedValue())
			err1 := err.(*Error)
			is(err1.message, "Cannot access member 'prop' of null")
			is(len(err1.trace), 1)
			is(err1.trace[0].location(), "C (file1.js:7:5)")
		}


	})
}

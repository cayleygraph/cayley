$SCRIPTPATH=pwd

$env:GOPATH = $SCRIPTPATH

echo "Fetching dependencies to $SCRIPTPATH..."
echo "                  (00/15)"
  go get -u -t github.com/smartystreets/goconvey
echo "#                 (01/15)"
  go get -u github.com/badgerodon/peg
echo "##                (02/15)"
  go get -u github.com/barakmich/glog
echo "####              (03/15)"
  go get -u github.com/julienschmidt/httprouter
echo "#####             (04/15)"
  go get -u github.com/petar/GoLLRB/llrb
echo "######            (05/15)"
  go get -u github.com/robertkrimen/otto
echo "#######           (06/15)"
  go get -u github.com/stretchrcom/testify
echo "########          (07/15)"
  go get -u github.com/syndtr/goleveldb/leveldb
echo "#########         (08/15)"
  go get -u github.com/syndtr/goleveldb/leveldb/cache
echo "##########        (09/15)"
  go get -u github.com/syndtr/goleveldb/leveldb/iterator
echo "###########       (10/15)"
  go get -u github.com/syndtr/goleveldb/leveldb/opt
echo "############      (11/15)"
  go get -u github.com/syndtr/goleveldb/leveldb/util
echo "#############     (12/15)"
  go get -u labix.org/v2/mgo
echo "##############    (13/15)"
  go get -u labix.org/v2/mgo/bson
echo "###############   (14/15)"
  go get -u github.com/russross/blackfriday
echo "################  (15/15)"
echo "\n"
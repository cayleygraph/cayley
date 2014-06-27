$SCRIPTPATH=pwd

$env:GOPATH = $SCRIPTPATH

echo "Building cayley"
go build cayley
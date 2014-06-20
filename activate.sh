# Absolute path this script is in. /home/user/bin
cd "`dirname '${BASH_SOURCE:-$0}'`"
SCRIPTPATH="`pwd`"
echo $dir
cd - > /dev/null

export GOPATH=$SCRIPTPATH
#export GOOS="linux"
#export GOARCH="amd64"

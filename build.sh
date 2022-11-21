# !/bin/bash
#chmod +x build.sh
#./build.sh "query-executor" "N3B" "dev"
echo "Container name:$1"
echo "Container local tag:$2"
echo "Container repository tag:$3"
echo "Container expose port:$4"
SERVICE_PORT=$4
CNTNAME="$1:$2"
echo $CNTNAME

BUILDCMD="docker build -f prod.dockerfile -t $CNTNAME --progress plain --build-arg SERVICE_PORT=$SERVICE_PORT ."
echo $BUILDCMD
eval $BUILDCMD

BUILDCMD="docker tag $CNTNAME 10.50.4.110:5000/$1:$3"
echo $BUILDCMD
eval $BUILDCMD

BUILDCMD="docker push 10.50.4.110:5000/$1:$3"
echo $BUILDCMD
eval $BUILDCMD


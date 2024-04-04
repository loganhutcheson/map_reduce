if [ -z $1 ]; then
	echo "Please provide number of workers!"
	exit 1
fi

echo "Cleaning workspace..."
rm *temp* 2>/dev/null
echo "Compiling..."
go build -buildmode=plugin ../mrapps/wc.go
echo "Starting coordinator..."
go run mrcoordinator.go pg-*.txt &
echo "Starting $1 workers..."
./create_workers.sh $1

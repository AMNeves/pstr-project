docker pull nunopreguica/ps1819-kafka

docker rm -f kafka

docker run -h kafka --name=kafka --network=ps-net --rm -t  -p 9092:9092 -p 2181:2181 nunopreguica/ps1819-kafka
pause
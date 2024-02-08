# Trade data enrichment
Service enriches trade data with product names from the static data file

## Requirements
- Java 17+
- Maven 3.9.5

## Build and execute instructions
- Build
 ```shell
mvn clean install 
```
- Run
```shell
java -jar trade.data.enrichment-1.0.0.jar --product.file={product.csv file location} -cp {semicolon separated dependencies}
```

## Instructions for use
- Enrich trade data with product names
```shell
curl --request POST -F file=@{trade.csv file location} http://localhost:8080/api/v1/enrich --header 'Content-Type:text/csv' --header 'Accept:text/csv'
```

## Possible improvements
For very large product files that exceed the available memory, the ProductRepository interface may be implemented
in a different way. For example, using a file database or using an external database engine or search engine.
# simpledb-bkrs
##SimpleDB backup and restore

Console program to backup and restore SimpleDB domains. With this program, you can:

* Clone a SimpleDB domain to another domain
* Export a SimpleDB domain into a CSV file
* Import a previously exported CSV file into a SimpleDB domain

## Building

You will need:

* Java 1.8
* Maven (tested with 3.3.1)

```
mvn clean install
```

## Usage

Running just the program without parameters will output the usage message:
```
java -jar simpledb-bkrs-1.0.0-jar-with-dependencies.jar
usage: java ...
 -d,--destination <domain>   the destination to backup to. Examples:
                             file:///home/van/backup.csv,
                             file:///c:/temp/backup.csv (Windows),
                             sdb:///backupdomain
 -s,--source <domain>        the source to backup from. Examples:
                             sdb:///mydomain, file:///home/van/backup.csv,
                             file:///c:/temp/backup.csv (Windows)

```

### Examples

#### Back up a domain
`java -jar simpledb-bkrs-1.0.0-jar-with-dependencies.jar -s sdb:///MyDomain -d sdb:///MyBackup`

#### Export domain to a CSV
```
java -jar simpledb-bkrs-1.0.0-jar-with-dependencies.jar -s sdb:///MyDomain -d file:///home/van/mydomain.csv
```
or for **Windows**:
```
java -jar simpledb-bkrs-1.0.0-jar-with-dependencies.jar -s sdb:///MyDomain -d file:///c:/temp/van/mydomain.csv
```

#### Import a CSV into a domain
```
java -jar simpledb-bkrs-1.0.0-jar-with-dependencies.jar -s file:///home/van/mydomain.csv -d sdb:///MyBackupDomain
```

## Code
With CSV and SimpleDB implementations of the `org.van.providers.ItemSourceProvider` (data source)
and `org.van.providers.ItemStoreProvider` (data destination), they allow mix-and-match of implemenations to 
accommodate the scenarios above.

There is one unintended use case of copying the items from an exported CSV file to _another_ CSV file. It's
something academic but ultimately not a very compelling way to go about copying a CSV file. _I assume if you 
had wanted to make a copy of the file you'd use the `cp` or `copy` command that came with your OS_.

With implementations of the two abstract classes, most transfers are done by this snippet of code:
```
ItemSourceProvider source = ...;
ItemStoreProvider store = ...;

try ( ItemSourceProvider sourceProvider = source.initialize();
      ItemStoreProvider storeProvider = store.initialize()) {
        sourceProvider.iterateItems(storeProvider::storeItem);
}
```

That is, you create and initialize the source and store implementations. Then you basically call the 
`org.van.providers.ItemSourceProvider#iterateItems` with a `Consumer<com.amazonaws.services.simpledb.model.Item>`
to process the items from the source. And since `org.van.providers.ItemStoreProvider#storeItem` fits that signature,
the code above is all you need to transfer the items between them.



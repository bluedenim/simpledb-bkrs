package org.van;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.van.providers.ItemStoreProvider;
import org.van.providers.impl.*;
import org.van.providers.ItemSourceProvider;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.Optional;


public class SimpleDbBackupApplication {

    private static final Logger logger =
       Logger.getLogger(SimpleDbBackupApplication.class);

    public static void main(String[] args) {
        Options opts = new Options();
        opts.addOption(Option.builder("s")
            .longOpt("source")
            .argName("domain")
            .hasArg()
            .desc("the source to backup from. Examples: sdb:///mydomain, file:///home/van/backup.csv, file:///c:/temp/backup.csv (Windows)")
            .required(true)
            .build())
            .addOption(Option.builder("d")
                .longOpt("destination")
                .argName("domain")
                .hasArg()
                .desc("the destination to backup to. Examples: file:///home/van/backup.csv, file:///c:/temp/backup.csv (Windows), sdb:///backupdomain")
                .required(true)
                .build())
        ;
        try {
            CommandLine cmdLine = new DefaultParser().parse(opts, args);
            URI sourceUrl = new URI(cmdLine.getOptionValue("source"));
            URI destUrl = new URI(cmdLine.getOptionValue("destination"));

            new SimpleDbBackupApplication().run(sourceUrl, destUrl);
        } catch (MissingOptionException|MissingArgumentException ex) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java ...", opts);
        } catch (Exception ex) {
            logger.error(String.format("Error encountered. Check: 1) URIs' format (run without parameters to see help), 2) SimpleDB accessibility, and 3) AWS configuration.", ex));
            System.exit(1);
        }
    }

    public void run(URI sourceUri, URI destinationUri) throws Exception {
        File f = File.createTempFile("simpledb-bkrs", ".csv");
        try {
            ItemSourceProvider source = SourceProviderFactory.sourceProviderFor(sourceUri);
            ItemStoreProvider store = SourceProviderFactory.storeProviderFor(destinationUri);

            try (ItemSourceProvider sourceProvider = source.initialize();
                 ItemStoreProvider storeProvider = store.initialize()
            ) {
                logger.info(String.format("Transferring content from %s to %s...",
                    sourceUri, destinationUri));
                sourceProvider.iterateItems(storeProvider::storeItem);
                logger.info("Transfer complete");
            }
        } finally {
            FileUtils.deleteQuietly(f);
        }
    }
}

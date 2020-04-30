package awsservices;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.*;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class Glue {

    private static AWSGlue glueClient;
    private static final Logger logger = Logger.getLogger(Glue.class);

    public static void updateTableMetadata(
            String region,
            String dbName,
            String tableName,
            String statsPrefix,
            HashMap<String, String> tableParams,
            HashMap<String, HashMap<String, String>> columnsParams) throws Exception{

        if((tableParams == null || tableParams.size() == 0) && (columnsParams == null || columnsParams.size() == 0) ){
            logger.info("DB: "+dbName+" - Table: "+tableName+" /// no table or column metrics found. Skipping.");
            return;
        }

        logger.info("START: Updating metadata for DB: "+dbName+" /// Table: "+tableName);

        getGlueClient(region);

        GetTableRequest tblRequest = new GetTableRequest()
                .withDatabaseName(dbName)
                .withName(tableName);

        GetTableResult tblResult = glueClient.getTable(tblRequest);
        Table table = tblResult.getTable();

        TableInput tableInput = copyTableToTableInput(table);

        /*
            Prepare table level metadata updates
         */
        if(tableParams != null && tableParams.size() > 0){

            tableParams.forEach((k,v) -> {
                if(tableInput.getParameters().containsKey(k)){
                    logger.debug("DB: "+dbName+" /// Table: "+tableName+" /// Updating table parameter: "+k+" with value: "+v);
                    tableInput.getParameters().put(k,v);
                }
                else {
                    logger.debug("DB: "+dbName+" /// Table: "+tableName+" /// Adding table parameter: "+k+" with value: "+v);
                    tableInput.addParametersEntry(k, v);
                }
            });

        }

        /*
            Prepare column level metadata updates
         */
        if(columnsParams != null && columnsParams.size() > 0){

            List<Column> columns = tableInput.getStorageDescriptor().getColumns();

            /*
            Glue does not support parameters for partition columns
            com.amazonaws.services.glue.model.InvalidInputException: Parameters not supported for partition columns
            (Service: AWSGlue; Status Code: 400; Error Code: InvalidInputException; Request ID: df2ec4e2-2e10-11ea-9310-cdcdb47af5cc)
             */
            /*
            List<Column> partitions = tableInput.getPartitionKeys();
            if(partitions.size() > 0){
                columns.addAll(partitions);
            }
            */

            columnsParams.forEach((k, v) -> {

                Optional<Column> optCol = columns.stream().filter(c -> c.getName().trim().equals(k.trim())).findFirst();

                if(optCol.isPresent()){

                    Column col2u = optCol.get();

                    //Remove parameters from column if already available, this enables parameter update, not allowed by
                    //addParametersEntry method on column and also resets profiler parameters from previous runs.
                    if(col2u.getParameters() != null){

                        List<String> toBeRem = new ArrayList<>();

                        col2u.getParameters().forEach((pName, pValue) -> {
                            if(pName.startsWith(statsPrefix)){
                                toBeRem.add(pName);
                            }
                        });

                        toBeRem.forEach(pName -> {
                            logger.debug("DB: "+dbName+" /// Table: "+tableName+" /// Column: "+col2u.getName()+" /// Removing parameter: "+pName+" for update.");
                            col2u.getParameters().remove(pName);
                        });
                    }

                    //Add/Update parameter
                    v.forEach((pName, pValue) -> {
                        logger.debug("DB: "+dbName+" /// Table: "+tableName+" /// Column: "+col2u.getName()+" /// Adding/Updating parameter: "+pName+", Value: "+pValue);
                        col2u.addParametersEntry(pName, pValue);
                    });

                }
                else {
                    logger.error("Column Not Found: "+k);
                }

            });
        }

        /*
            Update Metadata
         */
        UpdateTableRequest updateTableRequest = new UpdateTableRequest()
                .withDatabaseName(dbName)
                .withTableInput(tableInput);

        glueClient.updateTable(updateTableRequest);

        logger.info("END: Updating metadata for DB: "+dbName+" /// Table: "+tableName);
    }


    public static List<Table> getDBTables(String region, String dbName){

        GetTablesRequest tblRequest = new GetTablesRequest().withDatabaseName(dbName);

        getGlueClient(region);

        GetTablesResult tblResults = glueClient.getTables(tblRequest);

        List<Table> tables = tblResults.getTableList();

        if(logger.isDebugEnabled()){
            tables.forEach(t -> {
                logger.debug(t.getName());
            });
        }

        return tables;
    }


    private static TableInput copyTableToTableInput(Table table){

        return new TableInput()
                .withDescription(table.getDescription())
                .withLastAccessTime(table.getLastAccessTime())
                .withLastAnalyzedTime(table.getLastAnalyzedTime())
                .withName(table.getName())
                .withOwner(table.getOwner())
                .withParameters(table.getParameters())
                .withPartitionKeys(table.getPartitionKeys())
                .withRetention(table.getRetention())
                .withStorageDescriptor(table.getStorageDescriptor())
                .withTableType(table.getTableType())
                .withViewExpandedText(table.getViewExpandedText())
                .withViewOriginalText(table.getViewOriginalText())
                ;
    }


    public static AWSGlue getGlueClient(String region){
        return getGlueClient(Regions.fromName(region));
    }


    public static AWSGlue getGlueClient(Regions region){

        if (glueClient == null ){
            glueClient = AWSGlueClient.builder()
                    .withRegion(region)
                    .build();
        }

        return glueClient;

    }

}

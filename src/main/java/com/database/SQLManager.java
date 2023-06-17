package com.database;

import com.database.Parser.operationNumber;

public class SQLManager {
    
    private FileManager fileM;
    private Parser parser;

    SQLManager(FileManager fm)
    {
        fileM = fm;
        parser = new Parser();
    }

    public void processStatement(String statement)
    {
        boolean success = false;
        parser.parse(statement);
        if(parser.getIsValid())
        {
            if(parser.getOperation() == operationNumber.SELECT)
                success = select();
            else if(parser.getOperation() == operationNumber.INSERT)
                success = insert();
            else if(parser.getOperation() == operationNumber.DELETE)
                success = delete();
            else if(parser.getOperation() == operationNumber.UPDATE)
                success = update();
        }
        if(!success)
            error();
    }

    public void error()
    {
        System.out.println("Error in SQL statement");
    }
    
    public boolean select()
    {
        SelectOperation operation = new SelectOperation(fileM, parser.getColumns(), parser.getFromTable(), parser.getForeignTables(), parser.getForeignKeys(), parser.getWhereStatement(), parser.getOrderByStatement());
        return operation.execute();
    }

    public boolean insert()
    {
        InsertOperation operation = new InsertOperation(fileM, parser.getInsertFields(), parser.getInsertValues(), parser.getFromTable());
        return operation.execute();
    }

    public boolean delete()
    {
        DeleteOperation operation = new DeleteOperation(fileM, parser.getFromTable(), parser.getWhereStatement());
        return operation.execute();
    }

    public boolean update()
    {
        UpdateOperation operation = new UpdateOperation(fileM, parser.getFromTable(), parser.getUpdateField(), parser.getUpdateValue(), parser.getWhereStatement());
        return operation.execute();
    }
}

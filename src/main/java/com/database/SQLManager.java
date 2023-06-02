package com.database;

import com.database.Parser.operationNumber;

public class SQLManager {
    
    private Parser parser;

    SQLManager()
    {
        parser = new Parser();
    }

    public void processStatement(String statement)
    {
        parser.parse(statement);
        if(parser.getIsValid())
        {
            if(parser.getOperation() == operationNumber.SELECT)
                select();
        }
        else
            System.out.println("Error in SQL statement");
    }
    
    public void select()
    {
        boolean success = false;
        SelectOperation operation = new SelectOperation(parser.getColumns(), parser.getFromTable());
        success = operation.execute();
        if(!success)
            System.out.println("Error in SQL statement");
    }
}

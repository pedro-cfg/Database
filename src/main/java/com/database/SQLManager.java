package com.database;

import com.database.Parser.operationNumber;
import java.awt.Color;

public class SQLManager {
    
    private FileManager fileM;
    private Parser parser;

    SQLManager(FileManager fm)
    {
        fileM = fm;
        parser = new Parser();
    }

    public void processStatement(javax.swing.JTextField jTextField, javax.swing.JTable jTable1, String statement)
    {
        boolean success = false;
        parser.parse(statement);
        if(parser.getIsValid())
        {
            long startTime = System.currentTimeMillis();
            if(parser.getOperation() == operationNumber.SELECT)
                success = select(jTable1);
            else if(parser.getOperation() == operationNumber.INSERT)
                success = insert();
            else if(parser.getOperation() == operationNumber.DELETE)
                success = delete();
            else if(parser.getOperation() == operationNumber.UPDATE)
                success = update();
            long endTime = System.currentTimeMillis(); // Captura o tempo final
            long elapsedTime = endTime - startTime;
            jTextField.setText("Success! Elapsed Time (" + Long.toString(elapsedTime) + "ms)");
            jTextField.setForeground(Color.GREEN);
        }
        if(!success)
        {
            jTextField.setText("Error in SQL statement");
            jTextField.setForeground(Color.RED);
        }
    }
    
    public boolean select(javax.swing.JTable jTable1)
    {
        SelectOperation operation = new SelectOperation(jTable1, fileM, parser.getColumns(), parser.getFromTable(), parser.getForeignTables(), parser.getForeignKeys(), parser.getWhereStatement(), parser.getOrderByStatement());
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

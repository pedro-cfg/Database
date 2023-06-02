package com.database;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    
    public enum operationNumber {
        SELECT,
        UPDATE,
        INSERT,
        DELETE
    }

    private operationNumber operation;
    private String sentence;
    private List<String> columns;
    private String fromTable;
    private boolean senteceIsValid;

    Parser()
    {
        clearSentence();
    }

    private void clearSentence()
    {
        operation = null;
        sentence = "";
        columns = new ArrayList<>();
        fromTable = "";
        senteceIsValid = false;
    }

    public operationNumber getOperation()
    {
        return operation;
    }

    public boolean parse(String SQLsentence)
    {
        clearSentence();

        sentence = SQLsentence;
        sentence = sentence.replaceAll(","," ");

        String[] words = sentence.split("\\s+");

        switch(words[0])
        {
            case "select":
                operation = operationNumber.SELECT;
                senteceIsValid = parseSelect(words);
                break;
            case "update":
                operation = operationNumber.SELECT;
                senteceIsValid = parseUpdate(words);
                break;
            case "insert":
                operation = operationNumber.SELECT;
                senteceIsValid = parseInsert(words);
                break;
            case "delete":
                operation = operationNumber.SELECT;
                senteceIsValid = parseDelete(words);
                break;
            default:
                return false;
        }
        
        return senteceIsValid;
    }

    public boolean parseSelect(String[] words)
    {
        int i = 1;
        while(i < words.length && !words[i].equals("from")) 
        {
            columns.add(words[i]);
            i++;
        }
        if(i == words.length)
            return false;
        i++;
        parseFrom(words, i);
        return true;
    }

    public boolean parseUpdate(String[] words)
    {
        // int i = 0;
        // while(i < words.length && !words[i].equals("from")) 
        // {
        //     updateClause = updateClause.concat(words[i] + " ");
        //     i++;
        // }
        return true;
    }

    public boolean parseInsert(String[] words)
    {
        // int i = 0;
        // while(i < words.length && !words[i].equals("from")) 
        // {
        //     insertClause = insertClause.concat(words[i] + " ");
        //     i++;
        // }
        return true;
    }

    public boolean parseDelete(String[] words)
    {
        // int i = 0;
        // while(i < words.length && !words[i].equals("from")) 
        // {
        //     deleteClause = deleteClause.concat(words[i] + " ");
        //     i++;
        // }
        return true;
    }

    public void parseFrom(String[] words, int i)
    {
        fromTable = words[i];
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public String getFromTable()
    {
        return fromTable;
    }

    public boolean getIsValid()
    {
        return senteceIsValid;
    }
}

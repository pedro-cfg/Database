package com.database;

import java.util.ArrayList;
import java.util.List;

public class SQLOperation 
{
    protected FileManager fileM;
    protected String fromTable;
    protected List<String> whereKeys;
    protected List<FileManager.Field> header;
    protected List<List<String>> results;
    protected List<List<String>> finalResults;

    SQLOperation(FileManager fm)
    {
        fileM = fm;
        results = new ArrayList<>();
        header = new ArrayList<>();
        finalResults = new ArrayList<>();
    }


    protected boolean whereClause()
    {
        int i = 0;
        while(i < whereKeys.size())
        {
            FileManager.Field field = fileM.new Field();;
            String operator = "";
            String value = "";
            List<List<String>> partialResults = new ArrayList<>();
            if(whereKeys.get(i).toLowerCase().equals("and"))
                i++;
            if(i < whereKeys.size() && whereKeys.get(i).contains("."))
            {
                field.table = whereKeys.get(i).split("\\.")[0];
                field.value = whereKeys.get(i).split("\\.")[1];
            }
            else
            {
                boolean found = false;
                for(int j = 0; j< header.size(); j++)
                {
                    if(whereKeys.get(i).equals(header.get(j).value))
                    {
                        field = header.get(j);
                        found = true;
                    }
                }
                if(!found)
                return false;
            }
            i++;
            if(i < whereKeys.size())
                operator = whereKeys.get(i);
            else
                return false;
            i++;
            if(i < whereKeys.size())
                value = whereKeys.get(i);
            else
                return false;
            if(!whereExecute(partialResults, field, operator, value))
                return false;
            i++;
            if(i < whereKeys.size() && whereKeys.get(i).toLowerCase().equals("or"))
            {
                List<List<String>> partialResults2 = new ArrayList<>();
                i++;
                if(whereKeys.get(i).contains("."))
                {
                    field.table = whereKeys.get(i).split("\\.")[0];
                    field.value = whereKeys.get(i).split("\\.")[1];
                }
                else
                {
                    boolean found = false;
                    for(int j = 0; j< header.size(); j++)
                    {
                        if(whereKeys.get(i).equals(header.get(j).value))
                        {
                            field = header.get(j);
                            found = true;
                        }
                    }
                    if(!found)
                    return false;
                }
                i++;
                if(i < whereKeys.size())
                    operator = whereKeys.get(i);
                else
                    return false;
                i++;
                if(i < whereKeys.size())
                    value = whereKeys.get(i);
                else
                    return false;
                if(!whereExecute(partialResults2, field, operator, value))
                    return false;
                orExecute(partialResults, partialResults2);
                i++;
            }
            results = partialResults;
        }
        return true;
    }

    protected boolean whereExecute(List<List<String>> partialResults, FileManager.Field field, String operator, String value)
    {
        int columnNumber = -1;
        for(int i = 0; i< header.size(); i++)
        {
            if(field.table.equals(header.get(i).table) && field.value.equals(header.get(i).value))
                columnNumber = i;
        }
        if(columnNumber == -1)
            return false;
        Integer integerValue = 0;
        boolean isInteger = checkInteger(value);
        boolean isResultInteger = checkInteger(results.get(0).get(columnNumber));
        if(isInteger && isResultInteger)
            integerValue = Integer.parseInt(value);
        List<String> line;
        for(int j = 0; j < results.size(); j++)
        {
            line = results.get(j);
            if(isInteger && isResultInteger)
            {
                if(compareInt(Integer.parseInt(line.get(columnNumber)), integerValue, operator))
                    partialResults.add(line);
            }
            else
            {
                if(compareString(line.get(columnNumber).replace("\"", ""), value, operator))
                    partialResults.add(line);
            }
        }
        return true;
    }

    protected void orExecute(List<List<String>> partialResults1, List<List<String>> partialResults2)
    {
        for(int i = 0; i < partialResults2.size(); i++)
        {
            List<String> line2 = partialResults2.get(i);
            boolean alreadyIn = false;
            for(int j = 0; j< partialResults1.size(); j++)
            {
                List<String> line1 = partialResults1.get(j);
                boolean equalLine = true;
                for(int k = 0; k < line1.size(); k++)
                {
                    if(!line1.get(k).equals(line2.get(k)))
                        equalLine = false;
                }
                if(equalLine == true)
                    alreadyIn = true;
            }
            if(!alreadyIn)
                partialResults1.add(line2);
        }
    }

    protected boolean compareString(String value1, String value2, String operator)
    {
        switch (operator) {
            case ">":
                if (value1.compareTo(value2) > 0)
                    return true;
                break;
            case "<":
                if (value1.compareTo(value2) < 0)
                    return true;
                break;
            case ">=":
                if (value1.compareTo(value2) >= 0)
                    return true;
                break;
            case "<=":
                if (value1.compareTo(value2) <= 0)
                    return true;
                break;
            case "=":
                if (value1.equals(value2))
                    return true;
                break;
        }
        return false;
    }

    protected boolean compareInt(int value1, int value2, String operator)
    {
        switch(operator)
        {
            case ">":
                if(value1>value2)
                    return true;
                break;
            case "<":
                if(value1<value2)
                    return true;
                break;
            case ">=":
                if(value1>=value2)
                    return true;
                break;
            case "<=":
                if(value1<=value2)
                    return true;
                break;
            case "=":
                if(value1==value2)
                    return true;
                break;  
        }
        return false;
    }

    protected boolean checkInteger(String value)
    {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    protected String constructFormattedLine(List<String> line)
    {
        String formattedLine = "";
        for(int i = 0; i < line.size(); i++)
        {
            formattedLine = formattedLine.concat(line.get(i));
            if(i < line.size() - 1)
                formattedLine = formattedLine.concat(",");
        }
        return formattedLine;
    }

    protected void deleteAndConstruct()
    {
        fileM.eraseFile(fromTable);
        List<String> headerList = new ArrayList<>();
        for(int i = 0; i < header.size(); i++)
            headerList.add(header.get(i).value);
        String head = constructFormattedLine(headerList);
        fileM.appendLine(fromTable, head);
        String line = "";
        for(int i = 0; i < finalResults.size(); i++)
        {
            line = constructFormattedLine(finalResults.get(i));
            fileM.appendLine(fromTable, line);
        }
    }
}

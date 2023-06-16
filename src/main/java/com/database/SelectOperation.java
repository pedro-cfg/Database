package com.database;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SelectOperation {

    protected class Field
    {
        protected String table;
        protected String value;
    }

    private List<String> columns;
    private String fromTable;
    private List<String> joinTables;
    private List<String> joinKeys;
    private List<String> whereKeys;
    private List<String> orderKeys;
    private List<List<String>> results;
    private List<Integer> columnNumbers;
    private List<Field> header;

    SelectOperation(List<String> columnsList, String table, List<String> jTables, List<String> jKeys, List<String> wKeys, List<String> oKeys)
    {
        columns = columnsList;
        fromTable = table;
        joinTables = jTables;
        joinKeys = jKeys;
        whereKeys = wKeys;
        orderKeys = oKeys;
        results = new ArrayList<>();
        columnNumbers = new ArrayList<>();
        header = new ArrayList<>();
    }

    public boolean execute()
    {
        if(!buildHeader())
            return false;
        if(!buildResults())
            return false;
        if(whereKeys.size() > 0)
        {
            if(!whereClause())
                return false;
        }
        if(orderKeys.size() > 0)
        {
            if(!orderByClause())
                return false;
        }
        for(int i = 0; i < columns.size(); i++)
            System.out.print(header.get(columnNumbers.get(i)).value + " ");
        System.out.print("\n");
        printResults();
        return true;
    }

    public void printResults()
    {
        for(int i = 0; i < results.size(); i++)
        {
            for(int j = 0; j < columns.size(); j++)
            {
                System.out.print(results.get(i).get(columnNumbers.get(j)) + " ");
            }
            System.out.print("\n");
        }
    }

    public boolean buildResults()
    {
        String path = "data/" + fromTable + ".csv";
        Path pathToFile = Paths.get(path);
        try(BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8))
        {
            String line = br.readLine();
            String[] fields = line.split(",");
            line = br.readLine();
            while (line != null) {
                fields = line.split(",");
                List<String> list = new ArrayList<>();
                for(int i = 0; i < fields.length; i++)
                {
                    list.add(fields[i]);
                }
                results.add(list);
                line = br.readLine();
            }
            for(int i = 0; i < joinTables.size(); i++)
            {
                if(joinKeys.size()>joinTables.size())
                {
                    join(joinTables.get(i),joinKeys.get(i*2).split("\\.")[1]);
                }
                else
                {
                    join(joinTables.get(i),joinKeys.get(i));
                }
            }
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public boolean whereClause()
    {
        int i = 0;
        while(i < whereKeys.size())
        {
            Field field = new Field();
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

    public boolean whereExecute(List<List<String>> partialResults, Field field, String operator, String value)
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
        if(isInteger)
            integerValue = Integer.parseInt(value);
        List<String> line;
        for(int j = 0; j < results.size(); j++)
        {
            line = results.get(j);
            if(isInteger)
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

    public void orExecute(List<List<String>> partialResults1, List<List<String>> partialResults2)
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

    public boolean compareString(String value1, String value2, String operator)
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

    public boolean compareInt(int value1, int value2, String operator)
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

    public boolean checkInteger(String value)
    {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean orderByClause()
    {
        int i = orderKeys.size()-1;
        while(i >= 0)
        {
            String order = "desc";
            Field field = new Field();
            int position = 0;
            if(i >= 0)
            {
                if(orderKeys.get(i).toLowerCase().equals("asc") || orderKeys.get(i).toLowerCase().equals("desc"))
                    order = orderKeys.get(i);
            }
            i--;
            if(i >= 0 && orderKeys.get(i).contains("."))
            {
                field.table = orderKeys.get(i).split("\\.")[0];
                field.value = orderKeys.get(i).split("\\.")[1];
            }
            else
            {
                boolean found = false;
                for(int j = 0; j< header.size(); j++)
                {
                    if(orderKeys.get(i).equals(header.get(j).value))
                    {
                        field = header.get(j);
                        found = true;
                        position = j;
                    }
                }
                if(!found)
                    return false;
            }
            boolean found = false;
            for(int j = 0; j< header.size(); j++)
            {
                if(field.table.equals(header.get(j).table) && field.value.equals(header.get(j).value))
                {
                    position = j;
                    found = true;
                }
            }
            if(!found)
                return false;
            bubbleSort(position, order);
            i--;
        }
        return true;
    }

    public void bubbleSort(int position, String order)
    {
        int n = results.size();
        boolean swapped;

        if(results.size() > 0)
        {
            boolean isInteger = checkInteger(results.get(0).get(position));
            if(isInteger)
            {
                for (int i = 0; i < n - 1; i++) 
                {
                    swapped = false;
                    for (int j = 0; j < n - i - 1; j++) 
                    {
                        int firstValue = Integer.parseInt(results.get(j).get(position));
                        int secondValue = Integer.parseInt(results.get(j+1).get(position));
                        if(order.toLowerCase().equals("asc"))
                        {
                            if(firstValue > secondValue)
                            {
                                swap(j);
                                swapped = true;
                            }
                        }
                        else
                        {
                            if(firstValue < secondValue)
                            {
                                swap(j);
                                swapped = true;
                            }
                        }
                    }
                    if (!swapped) 
                        break;
                }
            }
            else
            {
                for (int i = 0; i < n - 1; i++) 
                {
                    swapped = false;
                    for (int j = 0; j < n - i - 1; j++) 
                    {
                        String firstValue = results.get(j).get(position);
                        String secondValue = results.get(j+1).get(position);
                        if(order.toLowerCase().equals("asc"))
                        {
                            if(firstValue.compareTo(secondValue) > 0)
                            {
                                swap(j);;
                                swapped = true;
                            }
                        }
                        else
                        {
                            if(firstValue.compareTo(secondValue) < 0)
                            {
                                swap(j);
                                swapped = true;
                            }
                        }
                    }
                    if (!swapped) 
                        break;
                }
            }
        }
    }

    public void swap(int j)
    {
        List<String> temp = results.get(j);
        results.set(j, results.get(j+1));
        results.set(j+1, temp);
    }

    public boolean buildHeader()
    {
        List<Field> mainHeader = getHeader(fromTable);
        if(mainHeader == null)
            return false;

        if(joinTables.size() == 0)
        {
            header = mainHeader;
            if(!buildSelect())
                return false;
            return true;
        }
        else if(joinKeys.size() > joinTables.size())
        {
            for(int i = 0; i < joinTables.size(); i++)
            {
                List<Field> joinHeader = getHeader(joinTables.get(i));
                if(!checkForeignKey(mainHeader, joinKeys.get(i*2)) && !checkForeignKey(mainHeader, joinKeys.get(i*2+1)))
                    return false;
                if(!checkForeignKey(joinHeader, joinKeys.get(i*2)) && !checkForeignKey(joinHeader, joinKeys.get(i*2+1)))
                    return false;
                mainHeader.addAll(joinHeader);
            }
            header = mainHeader;
            if(!buildSelect())
                return false;
            return true;
        }
        else if(joinKeys.size() == joinTables.size())
        {
            for(int i = 0; i < joinTables.size(); i++)
            {
                List<Field> joinHeader = getHeader(joinTables.get(i));
                if(!checkForeignKey(mainHeader, joinKeys.get(i)))
                    return false;
                if(!checkForeignKey(joinHeader, joinKeys.get(i)))
                    return false;
                mainHeader.addAll(joinHeader);
            }
            header = mainHeader;
            if(!buildSelect())
                return false;
            return true;
        }
        else
            return false;
    }

    public boolean buildSelect()
    {
        if(columns.get(0).equals("*"))
        {
            columns.clear();
            for(int i = 0; i < header.size(); i++)
            {
                columns.add(header.get(i).table + "." + header.get(i).value);
            }
        }
        boolean correspondent;
        for(int i = 0; i< columns.size(); i++)
        {
            Field field = new Field();
            if(columns.get(i).contains("."))
            {
                field.table = columns.get(i).split("\\.")[0];
                field.value = columns.get(i).split("\\.")[1];
            }
            else
            {
                field.value = columns.get(i);
            }               
            correspondent = false;
            for(int j = 0; j < header.size() && correspondent == false; j++)
            {
                if(header.get(j).value.equals(field.value))
                {
                    if(field.table == null || header.get(j).table.equals(field.table))
                    {
                        columnNumbers.add(j);
                        correspondent = true;
                    }
                }
            }
            if(!correspondent)
                return false;
        }
        return true;
    }

    public List<Field> getHeader(String table)
    {
        List<Field> head = new ArrayList<>();
        String path = "data/" + table + ".csv";
        Path pathToFile = Paths.get(path);
        try(BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8))
        {
            String line = br.readLine();
            String[] fields = line.split(",");        
            for(int i = 0; i < fields.length; i++)
            {
                Field field = new Field();
                field.table = table;
                field.value = fields[i];
                head.add(field);
            }
            return head;
        }
        catch (IOException e)
        {
            return null;
        }
    }

    public boolean checkForeignKey(List<Field> columns,String key)
    {
        if(columns == null)
            return false;
        Field field = new Field();
        if(key.contains("."))
        {
            String[] vector = key.split("\\.");
            field.table = vector[0];
            field.value = vector[1];
        }
        else
        {
            field.table = columns.get(0).table;
            field.value = key;
        }
        for(int i = 0; i < columns.size(); i++)
        {
            if(columns.get(i).table.equals(field.table) && columns.get(i).value.equals(field.value))
                return true;
        }
        return false;
    }

    public boolean join(String table, String key)
    {
        int firstKey = -1;
        int secondKey = -1;
        int initialSize = results.get(0).size();
        for(int i = 0; i < header.size(); i++)
        {
            if(header.get(i).value.equals(key))
            {
                if(firstKey == -1)
                    firstKey = i;
                if(header.get(i).table.equals(table))
                    secondKey = i;
            }
        }
        if(firstKey == -1 || secondKey == -1)
            return false;

        secondKey = secondKey - initialSize;
        for(int i = 0; i < results.size(); i++)
        {      
            String path = "data/" + table + ".csv";
            Path pathToFile = Paths.get(path);
            try(BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8))
            {
                String line = br.readLine();
                String[] fields = line.split(",");  
                line = br.readLine();
                while(line != null)
                {
                    fields = line.split(",");
                    List<String> list = new ArrayList<>();
                    for(int j = 0; j < fields.length; j++)
                    {
                        list.add(fields[j]);
                    }
                    if(fields[secondKey].equals(results.get(i).get(firstKey)))
                        results.get(i).addAll(list);
                    line = br.readLine();
                }
                if(results.get(i).size() <= initialSize)
                {
                    results.remove(i);
                    i--;
                }
            }
            catch (IOException e)
            {
                return false;
            }
        } 
        return true;       
    }
}

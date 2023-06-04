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
    private List<List<String>> results;
    private List<Integer> columnNumbers;
    private List<Field> header;

    SelectOperation(List<String> columnsList, String table, List<String> jTables, List<String> jKeys)
    {
        columns = columnsList;
        fromTable = table;
        joinTables = jTables;
        joinKeys = jKeys;
        results = new ArrayList<>();
        columnNumbers = new ArrayList<>();
        header = new ArrayList<>();
    }

    public boolean execute()
    {
        if(!buildHeader())
            return false;
        for(int i = 0; i < columns.size(); i++)
        {
            System.out.print(header.get(columnNumbers.get(i)).value + " ");
        }
        System.out.print("\n");
        if(!buildResults())
            return false;
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

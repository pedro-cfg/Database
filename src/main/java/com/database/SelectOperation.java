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

    private List<String> columns;
    private String fromTable;
    private List<String> joinTables;
    private List<String> joinKeys;
    private List<List<String>> results;
    private List<Integer> columnNumbers;
    private List<String> header;

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
        String path = "data/" + fromTable + ".csv";
        Path pathToFile = Paths.get(path);
        if(!buildHeader())
            return false;
        for(int i = 0; i < header.size(); i++)
        {
            System.out.print(header.get(i) + " ");
        }
        System.out.print("\n");
        try(BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8))
        {
            String line = br.readLine();
            String[] fields = line.split(",");
            line = br.readLine();
            while (line != null) {
                fields = line.split(",");
                for(int i = 0; i < columns.size(); i++)
                {
                    System.out.print(fields[columnNumbers.get(i)] + " ");
                }
                System.out.print("\n");
                line = br.readLine();
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
        List<String> mainHeader = getHeader(fromTable);
        if(mainHeader == null)
            return false;

        if(joinTables.size() == 0)
        {
            select(mainHeader);
        }
        else if(joinKeys.size() > joinTables.size())
        {
            for(int i = 0; i < joinTables.size(); i++)
            {
                List<String> joinHeader = getHeader(joinTables.get(i));
                if(!checkForeignKey(fromTable, mainHeader, joinKeys.get(i*2)) && !checkForeignKey(fromTable, mainHeader, joinKeys.get(i*2+1)))
                    return false;
                if(!checkForeignKey(joinTables.get(i), joinHeader, joinKeys.get(i*2)) && !checkForeignKey(joinTables.get(i), joinHeader, joinKeys.get(i*2+1)))
                    return false;
                mainHeader.addAll(joinHeader);
            }
            select(mainHeader);
        }
        return true;
    }

    public boolean select(List<String> mainHeader)
    {
        if(columns.get(0).equals("*"))
        {
            columns.clear();
            for(int i = 0; i < mainHeader.size(); i++)
            {
                columns.add(mainHeader.get(i));
            }
        }
        boolean correspondent;
        for(int i = 0; i< columns.size(); i++)
        {
            String table;
            String value;
            if(columns.get(i).contains("."))
            {
                table = columns.get(i).split(".")[0];
                value = columns.get(i).split(".")[1];
            }
            else
            {
                value = columns.get(i);
            }               
            correspondent = false;
            for(int j = 0; j < mainHeader.size() && correspondent == false; j++)
            {
                if(mainHeader.get(j).equals(value))
                {
                    header.add(mainHeader.get(j));
                    columnNumbers.add(j);
                    correspondent = true;
                }
            }
            if(!correspondent)
                return false;
        }
        return true;
    }

    public List<String> getHeader(String table)
    {
        List<String> head = new ArrayList<>();
        String path = "data/" + table + ".csv";
        Path pathToFile = Paths.get(path);
        try(BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8))
        {
            String line = br.readLine();
            String[] fields = line.split(",");        
            for(int i = 0; i < fields.length; i++)
                head.add(fields[i]);
            return head;
        }
        catch (IOException e)
        {
            return null;
        }
    }

    public boolean checkForeignKey(String table,List<String> columns,String key)
    {
        String t = "";
        String value = "";
        if(key.contains("."))
        {
            String[] vector = key.split("\\.");
            t = vector[0];
            value = vector[1];
        }
        for(int i = 0; i < columns.size(); i++)
        {
            if(table.equals(t) && columns.get(i).equals(value))
                return true;
        }
        return false;
    }
}

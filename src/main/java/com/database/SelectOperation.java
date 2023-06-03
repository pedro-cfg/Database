package com.database;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SelectOperation {

    private List<String> columns;
    private String fromTable;
    private List<String> joinTables;
    private List<String> joinKeys;

    SelectOperation(List<String> columnsList, String table, List<String> jTables, List<String> jKeys)
    {
        columns = columnsList;
        fromTable = table;
        joinTables = jTables;
        joinKeys = jKeys;
    }

    public boolean execute()
    {
        String path = "data/" + fromTable + ".csv";
        Path pathToFile = Paths.get(path);
        try(BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8))
        {
            String line = br.readLine();
            String[] fields = line.split(",");
            String head = "";
            if(columns.get(0).equals("*"))
            {
                columns.clear();
                for(int i = 0; i < fields.length; i++)
                {
                   columns.add(fields[i]);
                }

            }
            boolean correspondent;
            int[] columnsNumbers= new int[columns.size()];
            for(int j = 0; j< columns.size(); j++)
            {
                correspondent = false;
                for(int i = 0; i < fields.length && correspondent == false; i++)
                {
                    if(fields[i].equals(columns.get(j)))
                    {
                        head = head.concat(fields[i] + " ");
                        columnsNumbers[j] = i;
                        correspondent = true;
                    }
                }
                if(!correspondent)
                    return false;
            }
            System.out.println(head);
            line = br.readLine();
            while (line != null) {
                fields = line.split(",");
                for(int i = 0; i < columns.size(); i++)
                {
                    System.out.print(fields[columnsNumbers[i]] + " ");
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
}

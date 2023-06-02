package com.database;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileReader 
{
    private String tableName;
    private String path;
    private Path pathToFile;
    private List<String> columns = new ArrayList<>();
    private int tableSize;

    FileReader()
    {

    }

    public void readTable(String table)
    {
        tableName = table;
        path = "data/" + tableName + ".csv";
        pathToFile = Paths.get(path);
        try(BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8))
        {
            String line = br.readLine();
            String[] fields = line.split(",");
            tableSize = fields.length;
            for(int i = 0; i < tableSize; i++)
                columns.add(fields[i]);
            line = br.readLine();
            while (line != null) {
                fields = line.split(",");
                for(int i = 0; i < fields.length; i++)
                {
                    System.out.printf(fields[i] + " ");
                }
                System.out.printf("\n");
                line = br.readLine();
            }
        }
        catch (IOException e)
        {
            System.out.println(e);
        }
    }
}

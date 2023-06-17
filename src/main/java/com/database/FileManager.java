package com.database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileManager 
{
    public class Field
    {
        protected String table;
        protected String value;
    }

    FileManager()
    {

    }

    public boolean getHeader(final List<Field> header, String table)
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
            header.clear();
            header.addAll(head);
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public boolean getResults(final List<List<String>> results, String table)
    {
        String path = "data/" + table + ".csv";
        Path pathToFile = Paths.get(path);
        List<List<String>> partialresults = new ArrayList<>();
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
                partialresults.add(list);
                line = br.readLine();
            }
            results.clear();
            results.addAll(partialresults);
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public boolean appendLine(String table, String line)
    {
        String path = "data/" + table + ".csv";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path,true))) {
            bw.write(line);
            bw.newLine();
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public boolean eraseFile(String table)
    {
        String path = "data/" + table + ".csv";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path,false))) {
            bw.write("");
            //bw.newLine();
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}

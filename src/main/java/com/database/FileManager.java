package com.database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JComboBox;
import javax.swing.JList;

public class FileManager 
{
    private JComboBox<String> jComboBox1;
    private JList<String> jList1;
    private String schema;
    private String table;
    private String user;
    private String password;
    
    public class Field
    {
        protected String table;
        protected String value;
    }

    FileManager()
    {
        schema = "";
    }
    
    public boolean getHeader(final List<Field> header, String table)
    {
        List<Field> head = new ArrayList<>();
        String path = "data/" + schema + "/" + table + ".csv";
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
        String path = "data/" + schema + "/" + table + ".csv";
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
        String path = "data/" + schema + "/" + table + ".csv";
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
        String path = "data/" + schema + "/" + table + ".csv";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path,false))) {
            bw.write("");
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }
    
    public void setSwingElements(JComboBox combo, JList<String> jList)
    {
        jComboBox1 = combo;
        jList1 = jList;
        executeSwing();
    }
    
    public void executeSwing()
    {
        getSchemas();
        getTables();
    }
    
    private void getSchemas()
    {
        String directoryPath = "data/";

        File directory = new File(directoryPath);
        File[] folders = directory.listFiles(File::isDirectory);
        
        DefaultComboBoxModel model = new DefaultComboBoxModel();
        jComboBox1.setModel(model);

        if (folders != null) {
            for (File folder : folders) {
                String folderName = folder.getName();
                model.addElement(folderName);
            }
        }
        
        setSchema();
    }
    
    public void setSchema()
    {
        schema = jComboBox1.getSelectedItem().toString();
        getTables();
    }
    
    public void setTable()
    {
        table = jList1.getSelectedValue();
    }
    
    private void getTables()
    {
        String path = "data/" + schema + "/";

        File directory = new File(path);
        File[] tables = directory.listFiles();
        
        DefaultListModel model = new DefaultListModel();
        jList1.setModel(model);

        if (tables != null) {
            for (File t : tables) {
                String tableName = t.getName();
                if(tableName.contains(".csv"))
                {
                    tableName = tableName.replace(".csv", "");
                    model.addElement(tableName);
                }
            }
        }
        
        setTable();
    }
    
    public void createSchema(String name)
    {
        String path = "data/" + name;
        Path folder = Paths.get(path);
        try {
            Files.createDirectory(folder);
        } catch (IOException e) {
            System.out.println(e);
        }
        getSchemas();
    }
    
    public void dropTable()
    {
        String path = "data/" + schema + "/" + table + ".csv";

        File file = new File(path);

        if (file.exists()) 
            file.delete();   
        
        getTables();
    }
    
    public void dropSchema()
    {
        String path = "data/" + schema;
        File folder = new File(path);
        
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
        }
        folder.delete();
        getSchemas();
    }
    
    public void importTable(String sc, String tb)
    {
        String path = "data/" + schema + "/";
        
        String url = "jdbc:mysql://localhost:3306/" + sc;
        String query = "SELECT * FROM " + tb;

        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(path + tb + ".csv"))) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                writer.write(metaData.getColumnName(i));
                if (i < columnCount) {
                    writer.write(",");
                }
            }
            writer.newLine();

            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    writer.write(resultSet.getString(i));
                    if (i < columnCount) {
                        writer.write(",");
                    }
                }
                writer.newLine();
            }
            getTables();

        } catch (SQLException | IOException e) {
            System.out.println(e);
        }
    }
    
    public boolean setConnection(String u, String p)
    {
        user = u;
        password = p;
        return testConnection();
    }
    
    private boolean testConnection()
    {
        String url = "jdbc:mysql://localhost:3306/";
        try (Connection connection = DriverManager.getConnection(url, user, password))
        {
            connection.close();
            return true;
        }
        catch(SQLException e)
        {
            System.out.println(e);
            return false;
        }
    }
}

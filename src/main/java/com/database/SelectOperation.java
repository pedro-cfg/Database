package com.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;

public class SelectOperation extends SQLOperation
{

    private List<String> columns;
    private List<String> joinTables;
    private List<String> joinKeys;
    private List<String> orderKeys;
    private List<Integer> columnNumbers;
    private javax.swing.JTable jTable;

    SelectOperation(javax.swing.JTable jTable1, FileManager fm, List<String> columnsList, String table, List<String> jTables, List<String> jKeys, List<String> wKeys, List<String> oKeys)
    {
        super(fm);
        columns = columnsList;
        fromTable = table;
        joinTables = jTables;
        joinKeys = jKeys;
        whereKeys = wKeys;
        orderKeys = oKeys;
        columnNumbers = new ArrayList<>();
        jTable = jTable1;
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
        buildTable();
        return true;
    }

    private boolean buildResults()
    {
        if(!fileM.getResults(results, fromTable))
            return false;
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

    private boolean orderByClause()
    {
        int i = orderKeys.size()-1;
        while(i >= 0)
        {
            String order = "desc";
            FileManager.Field field = fileM.new Field();;
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

    private void bubbleSort(int position, String order)
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

    private void swap(int j)
    {
        List<String> temp = results.get(j);
        results.set(j, results.get(j+1));
        results.set(j+1, temp);
    }

    private boolean buildHeader()
    {
        List<FileManager.Field> mainHeader = new ArrayList<>();
        if(!fileM.getHeader(mainHeader,fromTable))
            return false;
        if(mainHeader.size() == 0)
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
                List<FileManager.Field> joinHeader = new ArrayList<>();
                if(!fileM.getHeader(joinHeader,joinTables.get(i)))
                    return false;
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
                List<FileManager.Field> joinHeader = new ArrayList<>();
                if(!fileM.getHeader(joinHeader,joinTables.get(i)))
                    return false;
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

    private boolean buildSelect()
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
            FileManager.Field field = fileM.new Field();;
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

    private boolean checkForeignKey(List<FileManager.Field> columns,String key)
    {
        if(columns == null)
            return false;
        FileManager.Field field = fileM.new Field();
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

    private boolean join(String table, String key)
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
            List<List<String>> joinResults = new ArrayList<>();
            if(!fileM.getResults(joinResults, table))
                return false;
            for(int j = 0; j < joinResults.size(); j++)
            {
                List<String> line = joinResults.get(j);
                if(line.get(secondKey).equals(results.get(i).get(firstKey)))
                    results.get(i).addAll(line);
            }
            if(results.get(i).size() <= initialSize)
            {
                results.remove(i);
                i--;
            }
        } 
        return true;       
    }
    
    private void buildTable()
    {
        DefaultTableModel model = new DefaultTableModel();
        jTable.setModel(model);

        for (int i = 0; i < columns.size(); i++) {
            model.addColumn(header.get(columnNumbers.get(i)).value);
        }

        for(int i = 0; i < results.size(); i++)
        {
            List<String> line = new ArrayList<>();
            for(int j = 0; j < columns.size(); j++)
            {
                line.add(results.get(i).get(columnNumbers.get(j)));
            }
             model.addRow(line.toArray());
        }
       
    }
}

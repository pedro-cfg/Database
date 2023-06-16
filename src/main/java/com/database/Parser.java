package com.database;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private List<String> joinTables;
    private List<String> joinKeys;
    private List<String> whereKeys;
    private List<String> orderKeys;
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
        joinTables = new ArrayList<>();
        joinKeys = new ArrayList<>();
        whereKeys = new ArrayList<>();
        orderKeys = new ArrayList<>();
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

        switch(words[0].toLowerCase())
        {
            case "select":
                operation = operationNumber.SELECT;
                senteceIsValid = parseSelect(words);
                break;
            case "update":
                operation = operationNumber.UPDATE;
                senteceIsValid = parseUpdate(words);
                break;
            case "insert":
                operation = operationNumber.INSERT;
                senteceIsValid = parseInsert(words);
                break;
            case "delete":
                operation = operationNumber.DELETE;
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
        while(i < words.length && !words[i].toLowerCase().equals("from")) 
        {
            columns.add(words[i]);
            i++;
        }
        if(i == words.length)
            return false;
        i++;
        parseFrom(words, i);
        i++;
        if(i < words.length && (words[i].toLowerCase().equals("join") || (words[i].toLowerCase().equals("inner") && words[i++].toLowerCase().equals("join"))))
        {
            if(!parseJoin(words, i))
                return false;
            i++;
        }
        while(i < words.length && !words[i].toLowerCase().equals("order"))
        {
            if(words[i].toLowerCase().equals("where"))
            {
                if(!parseWhere(words, i))
                    return false;
            }
            i++;
        }
        while(i < words.length)
        {
            if(words[i].toLowerCase().equals("order"))
            {
                i++;
                if(words[i].toLowerCase().equals("by"))
                {
                    i++;
                    parseOrderBy(words, i);
                }
                else
                    return false;
            }
            i++;
        }
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

    public boolean parseJoin(String[] words, int i)
    {
        while(i<words.length && !words[i].toLowerCase().equals("where") && !words[i].toLowerCase().equals("order"))
        {
            if(words[i].toLowerCase().equals("join") || (words[i].toLowerCase().equals("inner") && words[i++].toLowerCase().equals("join")))
            {
                if(words[i].equals("inner"))
                    i++;
                i++;
            }
            joinTables.add(words[i]);
            i++;

            if(words[i].toLowerCase().equals("on"))
            {
                i++;
                if(words[i].contains("="))
                {
                    String[] separated = words[i].split("=");
                    joinKeys.add(separated[0]);
                    if(separated.length > 1 && !separated[1].equals(""))
                        joinKeys.add(separated[1]);
                    else
                    {
                        i++;
                        joinKeys.add(words[i]);
                    }
                }
                else
                {
                    joinKeys.add(words[i]);
                    i++;
                    if(words[i].contains("="))
                    {
                        words[i] = words[i].replaceAll("=","");
                        if(words[i].equals(""))
                            i++;
                        joinKeys.add(words[i]);
                    }
                    else
                        return false;
                }
            }
            else if(words[i].toLowerCase().contains("using"))
            {
                if(words[i].contains("("))
                {
                    words[i] = words[i].replaceAll("\\("," ");
                    words[i] = words[i].replaceAll("\\)","");
                    String[] separated = words[i].split(" ");
                    joinKeys.add(separated[1]);
                }
                else
                {
                    i++;
                    words[i] = words[i].replaceAll("\\(","");
                    words[i] = words[i].replaceAll("\\)","");
                    joinKeys.add(words[i]);
                }
            }
            else
                return false;
            i++;
        }
        return true;
    }

    public boolean parseWhere(String[] words, int i)
    {
        i++;
        while(i < words.length && !words[i].toLowerCase().equals("order"))
        {
            if(i < words.length && (words[i].toLowerCase().equals("and") || words[i].toLowerCase().equals("or")))
            {
                whereKeys.add(words[i]);
                i++;
            }
            String phrase = words[i];
            String regex = "(\\w+[.]\\w+)\\s*([<>=]+)\\s*(\\w+.*?)";
            if(!phrase.contains("."))
            {
                regex = "(\\w+)\\s*([<>=]+)\\s*(\\w+.*?)";
            }
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(phrase);
            int k=0;
            while(i < words.length && !matcher.matches() && k < 2)
            {
                k++;
                i++;
                if(i < words.length && words[i].contains("\""))
                {
                    String value = words[i];
                    Pattern pattern2 = Pattern.compile("\"(.*?)\"");;
                    Matcher matcher2 = pattern2.matcher(value);
                    while(i < words.length && !matcher2.matches())
                    {
                        i++;
                        if(i < words.length)
                            value = value.concat(" " + words[i]);
                        matcher2 = pattern2.matcher(value);
                    }
                    value = value.replace("\"","");
                    phrase = phrase.concat(" " + value);
                }
                else
                    phrase = phrase.concat(" " + words[i]);
                matcher = pattern.matcher(phrase);
            }
            if (matcher.matches()) {
                whereKeys.add(matcher.group(1));
                whereKeys.add(matcher.group(2));
                whereKeys.add(matcher.group(3));                
            } else {
                return false;
            }
            i++;
        }
        return true;
    }

    public void parseOrderBy(String[] words, int i)
    {
        while(i < words.length)
        {
            orderKeys.add(words[i]);
            i++;
            if(i >= words.length || (!words[i].toLowerCase().equals("desc") && !words[i].toLowerCase().equals("asc")))
            {
                orderKeys.add("asc"); 
            }        
            else
            {
                orderKeys.add(words[i]);
                i++;
            }
        }
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

    public List<String> getForeignTables()
    {
        return joinTables;
    }

    public List<String> getForeignKeys()
    {
        return joinKeys;
    }

    public List<String> getWhereStatement()
    {
        return whereKeys;
    }

    public List<String> getOrderByStatement()
    {
        return orderKeys;
    }
}

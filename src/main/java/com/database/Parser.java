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
    private List<String> insertFields;
    private List<String> insertValues;
    private String updateField;
    private String updateValue;

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
        insertFields = new ArrayList<>();
        insertValues = new ArrayList<>();
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
        sentence = sentence.replaceAll("\\("," ");
        sentence = sentence.replaceAll("\\)"," ");
        sentence = sentence.replaceAll(";"," ");

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

    private boolean parseSelect(String[] words)
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
        if(i < words.length && !words[i].toLowerCase().equals("join") &&!words[i].toLowerCase().equals("where") && !words[i].toLowerCase().equals("order"))
            return false;
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

    private boolean parseUpdate(String[] words)
    {
        int i = 1;
        if(i < words.length)
            fromTable = words[i];
        i++;
        if(i >= words.length || !words[i].toLowerCase().equals("set"))
            return false;
        i++;
        String phrase = words[i];
        String regex = "(\\w+[.]\\w+)\\s*[=]\\s*(\\w+.*?)";
        if(!phrase.contains("."))
        {
            regex = "(\\w+)\\s*[=]\\s*(\\w+.*?)";
        }
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(phrase);
        String word;
        String regex2 = "\"([^\"]+)\"";
        Pattern pattern2 = Pattern.compile(regex2);
        Matcher matcher2;
        i++;
        int k = 0;
        while(i < words.length && !matcher.matches() && k < 2)
        {
            if(words[i].contains("\""))
            {
                word = words[i];
                matcher2 = pattern2.matcher(word);
                while(i < words.length && !matcher2.matches())
                {
                    i++;
                    word = word.concat(" " + words[i]);
                    matcher2 = pattern2.matcher(word);
                }
                if(matcher2.matches())
                    phrase = phrase.concat(" " + word.replace("\"",""));
            }
            else
                phrase = phrase.concat(" " + words[i]);
            matcher = pattern.matcher(phrase);
            i++;
            k++;
        }
        if (matcher.matches()) {
            updateField = matcher.group(1);
            updateValue = matcher.group(2);                
        } else {
            return false;
        }
        if(i < words.length && words[i].toLowerCase().equals("where"))
        {
            if(!parseWhere(words, i))
            return false;
        }
        return true;
    }

    private boolean parseInsert(String[] words)
    {
        int i = 1;
        if(i >= words.length || !words[i].toLowerCase().equals("into"))
            return false;
        i++;
        if(i < words.length)
            fromTable = words[i];
        else
            return false;
        i++;
        while(i < words.length && !words[i].toLowerCase().equals("values"))
        {
            insertFields.add(words[i]);
            i++;
        }
        if(i >= words.length || !words[i].toLowerCase().equals("values"))
            return false;
        i++;
        String word;
        String regex = "\"([^\"]+)\"";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher;
        while(i < words.length)
        {
            if(words[i].contains("\""))
            {
                word = words[i];
                matcher = pattern.matcher(word);
                while(i < words.length && !matcher.matches())
                {
                    i++;
                    word = word.concat(" " + words[i]);
                    matcher = pattern.matcher(word);
                }
                if(matcher.matches())
                    insertValues.add(word.replaceAll("\"", ""));
            }
            else
                insertValues.add(words[i]);
            i++;
        }
        if((insertFields.size() > 0 && insertFields.size() != insertValues.size()) || insertValues.size() == 0)
            return false;
        return true;
    }

    private boolean parseDelete(String[] words)
    {
        int i = 1;
        if(i >= words.length || !words[i].toLowerCase().equals("from"))
            return false;
        i++;
        if(i < words.length)
            fromTable = words[i];
        else
            return false;
        i++;
        if(i >= words.length || !words[i].toLowerCase().equals("where"))
            return false;
        if(!parseWhere(words, i))
            return false;
        return true;
    }

    private boolean parseJoin(String[] words, int i)
    {
        while(i<words.length && !words[i].toLowerCase().equals("where") && !words[i].toLowerCase().equals("order"))
        {
            if(words[i].toLowerCase().equals("join") || (words[i].toLowerCase().equals("inner") && words[i++].toLowerCase().equals("join")))
            {
                if(words[i].equals("inner"))
                    i++;
                i++;
            }
            if(i < words.length)
                joinTables.add(words[i]);
            i++;

            if(i < words.length && words[i].toLowerCase().equals("on"))
            {
                i++;
                if(i < words.length && words[i].contains("="))
                {
                    String[] separated = words[i].split("=");
                    joinKeys.add(separated[0]);
                    if(separated.length > 1 && !separated[1].equals(""))
                        joinKeys.add(separated[1]);
                    else
                    {
                        i++;
                        if(i < words.length)
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
            else if(i < words.length && words[i].toLowerCase().contains("using"))
            {
                i++;
                joinKeys.add(words[i]);
            }
            else
                return false;
            i++;
        }
        return true;
    }

    private boolean parseWhere(String[] words, int i)
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
            String regex = "(\\w+[.]\\w+)([<>=]+)(\\w+.*?)";
            if(!phrase.contains("."))
            {
                regex = "(\\w+)([<>=]+)(\\w+.*?)";
            }
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(phrase);  
            boolean com = false;
            String value = "";
            while(i < words.length && !matcher.matches())
            {
                if(phrase.contains("\""))
                    com = true;
                i++;
                if(i < words.length && words[i].contains("\""))
                {
                    regex = "(\\w+)([<>=]+)\"(.*?)\"";
                    pattern = Pattern.compile(regex);
                    value = com?(" " + words[i]):(words[i]);
                    phrase = phrase.concat(value);
                    com = true;
                }
                else
                    if(i<words.length)
                    {
                        value = com?(" " + words[i]):(words[i]);
                        phrase = phrase.concat(value);
                    }
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

    private void parseOrderBy(String[] words, int i)
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

    private void parseFrom(String[] words, int i)
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

    public List<String> getInsertFields()
    {
        return insertFields;
    }

    public List<String> getInsertValues()
    {
        return insertValues;
    }

    public String getUpdateField()
    {
        return updateField;
    }

    public String getUpdateValue()
    {
        return updateValue;
    }
}

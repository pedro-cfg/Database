package com.database;

import java.util.Scanner;

public class Main {
    public static void main(String[] args)
    {   
        System.out.println("Hello World");
        
        SQLManager manager = new SQLManager();
        //FileReader fr = new FileReader();

        //fr.readTable("pessoa");

        Scanner scanner = new Scanner(System.in);

        System.out.println("Insert a SQL statement: ");
        String statement = scanner.nextLine();
        manager.processStatement(statement);

        scanner.close();
    }

}

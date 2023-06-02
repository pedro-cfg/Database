package com.database;

import java.util.Scanner;

public class Main {
    public static void main(String[] args)
    {           
        SQLManager manager = new SQLManager();

        Scanner scanner = new Scanner(System.in);

        boolean fim = false;

        while(!fim)
        {
            System.out.println("Insert a SQL statement: ");
            String statement = scanner.nextLine();
            manager.processStatement(statement);
        }

        scanner.close();
    }

}

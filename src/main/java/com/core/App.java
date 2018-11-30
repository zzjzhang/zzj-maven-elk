package com.core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;



/**
 * Spring boot 整合 elastic search
 *
 */
@SpringBootApplication
public class App {

	/**
	 * 程序入口
	 * 
	 */
    public static void main( String[] args ) {
    	SpringApplication.run(App.class, args);
        System.out.println( "Spring boot integrate elastic search starts!" );
    }

}
# WikipediaDumpData
A simple program to extract data from wikipedia XML dump files and display the name of contributors
and number of their edits.

To run the application you have to clone the repo. and run "mvn clean install", "mvn clean package" in the command line.
To import the project into your IDE you need to:
---> In case of Intellij:
Select “File” -> “Import Project”
Select root folder of your project
Select “Import project from external model”, select “Maven”
Leave default options and finish the import

--->in case of eclipse:
Select “File” -> “Import” -> “Maven” -> “Existing Maven Project”
Follow the import instructions

and then compile and run the java class WordCount.java

The output should appear in a folder called output which is generated after
running the code.

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class GenerateQueries {

	private static String TABLENAME = "Exmaple";
	static ArrayList<String> arLimited = new ArrayList();
	static ArrayList<String> arRandom = new ArrayList();
	int NoOfQueries=300; 

	public static void main(String[] args) {
		GenerateQueries gq = new GenerateQueries();
		gq.limitedScopeQueries();
		
		for(int i=0;i<arLimited.size();i++)
		System.out.println(arLimited.get(i));
		System.out.println(arLimited.size());
	}

	public void openAndReadFile() {
		try (BufferedReader br = new BufferedReader(new FileReader(
				"./../../all_month.csv"))) {

			String line = br.readLine();
			int count = 0;
			while (line != null) {
				line = br.readLine();
				arLimited = new ArrayList(Arrays.asList(line.split(",")));

				System.out.println(arLimited.get(1) + " : " + arLimited.get(2));

				if (count > 5)
					break;
				else
					count++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ArrayList limitedScopeQueries() {
		generateLimitedScopeQueries("latitude", 21, "longitude", 21);
		generateLimitedScopeQueries("mag", 10, "depth", 21);
		//generateLimitedScopeQueries("gap", 21, "nst", 21);
		return arLimited;
	}
	
	public ArrayList Queries() {
		//question7();
		question6();
		return arRandom;
	}
	
	public ArrayList question7() {mar
		
		for(int i=0;i<100;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and CLASS='0'");
		}
		
		for(int i=0;i<100;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and CLASS='1'");
		}
		
		for(int i=0;i<100;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and CLASS='2'");
		}
		
		for(int i=0;i<200;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and CLASS='3'");
		}
		return arRandom;
	}
	
public ArrayList question6() {
		
		for(int i=0;i<50;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and SEX='0' and SURVIVED='1'");
		}
		
		for(int i=0;i<50;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and  SEX='0' and SURVIVED='0'");
		}
		
		for(int i=0;i<50;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and  SEX='1' and SURVIVED='0'");
		}
		
		for(int i=0;i<50;i++) {
			arRandom.add("select AGE,SEX from venkat where AGE='0' and  SEX='1' and SURVIVED='1'");
		}
		return arRandom;
	}
	
	public ArrayList generateLimitedScopeQueries(String attribute1,int count1, String attribute2, int count2) {
		String query;
		int limit1,limit2=10;
		
		if(count1<=20)
			limit1=2;
		else
			limit1=10;
		
		//Single Attribute1
		for(int i=1;i<NoOfQueries;i++) {
			query = "select mag,magType from earthquakes where "+attribute1+" <((select max("+attribute1+") from earthquakes)- "+limit1+");";
			arLimited.add(query);
		}
		
		if(count2<=20)
			limit2=2;
		else
			limit2=10;
		
		//Single Attribute2
		for(int i=1;i<NoOfQueries;i++) {
		query = "select mag,magType from earthquakes where "+attribute2+" <((select max("+attribute2+") from earthquakes)- "+limit2+");";
		arLimited.add(query);
		}
		
		//double Attributes
		for(int i=1;i<NoOfQueries+100;i++){
			query = "select mag,magType from earthquakes where "+ attribute1+" <((select min("+attribute1+") from earthquakes)+"+limit1+") and "+attribute2+"< ((select min("+attribute2+") from earthquakes)+"+limit2+");";
			arLimited.add(query);
		}
		
		return arLimited;
		
	}
	
	public ArrayList generateRandomQueries(String attribute1,int min1,int max1,String attribute2, int min2, int max2) {
		String query;
		int limit1,limit2=10;
		
		Random rg = new Random();
		//Single Attribute1
		for(int i=1;i<NoOfQueries;i++) {
			limit1 = rg.nextInt(max1-min1)+min1;
			query = "select mag,magType from earthquakes where "+attribute1+" <((select max("+attribute1+") from earthquakes)- "+limit1+");";
			
			arRandom.add(query);
		}
		
		//Single Attribute2
		for(int i=1;i<NoOfQueries;i++) {
		limit2 = rg.nextInt(max2-min2)+min2;
		query = "select mag,magType from earthquakes where "+attribute2+" <((select max("+attribute2+") from earthquakes)- "+limit2+");";
		arRandom.add(query);
		}
		
		//double Attributes
		for(int i=1;i<NoOfQueries+100;i++){
			limit1 = rg.nextInt(max1-min1)+min1;
			limit2 = rg.nextInt(max2-min2)+min2;
			query = "select mag,magType from earthquakes where "+ attribute1+" <((select min("+attribute1+") from earthquakes)+"+limit1+") and "+attribute2+"< ((select min("+attribute2+") from earthquakes)+"+limit2+");";
			arRandom.add(query);
		}
		
		return arRandom;
	}
}

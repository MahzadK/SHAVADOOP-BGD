import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Projet MAPREDUCE SHAVADOOP
 * 
 * @author Charlotte ELI - Mahzad KALANTARI
 * @version 1.0
 */
public class Master {
	

/**
     * nb_worker
     * est le nombre de workers qui sont solicités
     */
	
	private int nb_worker ;
	
/**
     * nomFichier
     * Fichier input 
     */
	private String nomFichier;
	
	/**
     * nomFichierBin
     * l'endroit ou sont stockés les fichiers jar
     */
	private String NomFichierBin ;
	
	
	
/**
     * nomDossierShava
     * Le dossier ou va être enregistré tous les fichiers de Shavadoop
     * Ce dossier doit se trouver sur le réseau de TELECOM 
     * exemple : /cal/homes/mkalantari/Shavadoop
     */
	private  String nomDossierShava;
	
	
/**
     * liste des machines qui ont répondu OK
     * à l'appel du master
     * 
     */	
	private ArrayList<String> listeMachine;
	
/**
     * Dictionnaire <UMx, Machine>
     * 
     */	
    private HashMap<String, String> dicoUM_machine; 
    
    
    /**
     * nombre de reduceur>
     * 
     */	
    private int nb_reducer;     
    
    
    /**
     * Dictionnaire <Word, list<Um>>
     * 
     */	  
    private HashMap<String, ArrayList<String>> dicoWord_Um;

    /**
     * Dictionnaire <RMx, Machine>
     * 
     */	  
    private HashMap<String,String> dicoRMS_Machine;
    
    
    /**
     * Dictionnaire <Machines, Liste de mots>
     * 
     */	  
    private HashMap<Integer,ArrayList<String>> dicoMachine_Words;
    
    
    
    /**
     * user
     * nom du user
     */
	
	private String user ;

    
/**
     * Constructeur de Master
     * @param nb_worker
     * @param nbReducers
     * @param nomFichier
     * @param nomDossierShava
     * @param NomFichierBin
     * @param user
     * 
     */


	public Master(int nb_worker,int nbReducers, String nomFichier, 
			      String nomDossierShava,String NomFichierBin, String user){
		this.nb_worker = nb_worker;
		this.nomFichier = nomFichier;
		this.nomDossierShava = nomDossierShava;
		this.NomFichierBin = NomFichierBin;
		this.nb_reducer = nbReducers; 
		this.user = user;
	}

/**
     * Supression de tous les fichiers existants dans le dossier Shavadoop
     * @param nomDossierShava
     */

	static public void initaliseDossierShava(String nomDossierShava )
	{
	  File path = new File(nomDossierShava );
	  if( path.exists() )
	  {
	    File[] files = path.listFiles();
	    for( int i = 0 ; i < files.length ; i++ )
	    {
	      if( files[ i ].isDirectory() )
	      {
	    	  initaliseDossierShava( path+"\\"+files[ i ] );
	      }
	      files[ i ].delete();
	    }
	  }
	}
	
	/**
     * Supression de tous les fichiers existants dans le dossier Shavadoop
     * @param fichierMachine
     */

	public void setMachine(String fichierMachine)
	{
		
	System.out.println("\nDébut du test sur la connection ");
	int startTime = (int) System.currentTimeMillis();
		
	List<String> machines;
	ArrayList<TestConnectionSSH> listeTests = new ArrayList<TestConnectionSSH>();

	Path filein = Paths.get(fichierMachine);
	try {
		machines = Files.readAllLines(filein, Charset.forName("UTF-8"));
		for (String machine : machines) {
			/*
			 * on teste la connection SSH pendant 7 secondes maximum
			 */
			 TestConnectionSSH test = new TestConnectionSSH(machine, 7, this.user);
			test.start();
			listeTests.add(test);
		}
	} catch (IOException e1) {
		e1.printStackTrace();
	}

	ArrayList<String> liste_machines_ok = new ArrayList<String>();
	listeMachine = new ArrayList<String>();
	
	for (TestConnectionSSH test : listeTests) {
		try {
			test.join();// on attend la fin du test
			if (test.isConnectionOK()) {
				liste_machines_ok.add(test.getMachine());
				// on rajoute dans notre liste de machine OK:
				listeMachine.add(test.getMachine());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	Path file = Paths.get("liste_machines_OK.txt");
	try {
		Files.write(file, liste_machines_ok, Charset.forName("UTF-8"));
	} catch (IOException e) {
		e.printStackTrace();
	}

	int endTime = (int) System.currentTimeMillis();
	System.out.println("Fin du test sur la connection des machines => Exécution en " + new Integer(endTime-startTime).toString() + "ms");	
}
	
//création fichier à partir dun tableau
private void createFilefromArray(ArrayList<String> listeUM) throws IOException{
	FileUtils.deleteQuietly(new File(nomDossierShava+"/temp"));
	
	for (int i=0; i<listeUM.size();i++){
		String currentText = StringUtils.join(listeUM.get(i),"\n");
		File fileTmp = new File(nomDossierShava+"/temp");
	    FileUtils.write(fileTmp, currentText, true);
	
	}
	
	
	
}

	
	
/**
     * Définition du  dico <UMx, machine>
     */
	
	public void setDicoUmHost(){
		dicoUM_machine = new HashMap<String, String>();
		
	if(listeMachine.size()>=nb_worker) {
		for(int j = 0; j<nb_worker; j++)
		{
			String key = "UM" + new Integer(j).toString(); 
			String machine = listeMachine.get(j);
			dicoUM_machine.put(key, machine );
		}
	
	 }
	else {
		System.out.println(" Attention: le nombre de machines connectés est inférieur au nombre de workers");
	     }

	}
	
	
	//dicoMachine_Words
	
	public void setdicoMachine_Words(){
		
	dicoMachine_Words = new HashMap<Integer,ArrayList<String>>(); 
	
	int nb_word = dicoWord_Um.size();
	int nb_wordReducer = nb_word/nb_reducer;
	int reste = nb_word % nb_reducer;
	
	
	ArrayList<String> listeWordComplet  =  new ArrayList<String>();
	  for (String word: dicoWord_Um.keySet()) {
		  listeWordComplet.add(word);
	    }
	
	 
	if (reste == 0){

	    for (int i = 0; i<nb_reducer-1; i++ )	{
	    	
	    	ArrayList<String> listeWord =  new ArrayList<String>();
		    for (int j = i*nb_wordReducer;j<i*nb_wordReducer + nb_wordReducer; j++ ) 
		    {
		    	
			    listeWord.add(listeWordComplet.get(j));
			    //System.out.println(listeWordComplet.get(j));
		    }
		    dicoMachine_Words.put(i, listeWord);
	    }
	}
	else {
		
		nb_word = dicoWord_Um.size();
		nb_wordReducer = nb_word/(nb_reducer-1);
		reste = nb_word % (nb_reducer-1);
		//System.out.println("mod &&&&&& "+ mod);
		//int reste = nb_word- (nb_reducer*nb_wordReducer);
		
          for (int i = 0; i<nb_reducer-1; i++ )	{
	    	ArrayList<String> listeWord =  new ArrayList<String>();
		   
	    	for (int j = i*nb_wordReducer;j<i*nb_wordReducer + nb_wordReducer; j++ ) 
		    {
		    	
			    listeWord.add(listeWordComplet.get(j));
		    } 
		    dicoMachine_Words.put(i, listeWord);
	       }
          
		int lastI = nb_reducer-1;
		ArrayList<String> listeWord =  new ArrayList<String>();
		for (int j = nb_word-reste ; j<nb_word ; j++ )	{
			    
			    listeWord.add(listeWordComplet.get(j));

	    }
		 dicoMachine_Words.put(lastI, listeWord);
	}
		
}
	

	public void setDicoWordUM() throws IOException{
		dicoWord_Um = new HashMap<String, ArrayList<String>>();
		
		if(listeMachine.size()>=nb_worker) {
	
		   for(int j = 0; j<nb_worker; j++)
		{
			//lecture des fichiers Keyx dans le dossier shavadoop
			String dossierKey = this.nomDossierShava +"/Keys" + new Integer(j).toString(); 
			
			
			/* Ouverture et récupération des lignes */
			File keysFile = new File(dossierKey);
			List<String> lines = FileUtils.readLines(keysFile, new String("UTF-8"));
		
			/* Ajout de cette clé au dictionnaire */
			for(String key : lines)
			{
				/* Cas où c'est la première fois que l'on rencontre cette clé */
				if( dicoWord_Um.containsKey(key) == false )
				{
					ArrayList<String> tmpList = new ArrayList<String>();
					tmpList.add("UM"+new Integer(j).toString() );
					dicoWord_Um.put(key, tmpList);
				}
				
				/* Cas où la clé était déjà présente */
				else
					dicoWord_Um.get(key).add("UM"+new Integer(j).toString() );
			}
			
			//String machine = listeMachine.get(j);
			
		}
	}
		else {
			System.out.println(" Attention: le nombre de machines connectés est inférieur au nombre de workers");
		}
		
	
	}
	
	
	
/**
     * On définit le dico <Rmx, machine>
     */
	public void setdicoRMS_Machine(){
		dicoRMS_Machine = new HashMap<String, String>();
		
	if(listeMachine.size()>nb_reducer) {
		for(int j = 0; j<nb_reducer; j++)
		{
			String key = "RM" + new Integer(j).toString(); 
			String machine = listeMachine.get(j);
		    dicoRMS_Machine.put(key, machine );	   
		}
	
	 }
	else {
		System.out.println(" Attention: le nombre de machines connectés est inférieur au nombre de reducer");
	     }

	}
	

	
	
/**
     * Split du fichier en entrée.
     * @param nbLignes
     * ce paramètre correspond aux nombres de ligne que l'on veut splitter.
     */
	
	public void splitFichier(int nbLignes ) throws IOException
	{
		System.out.println("\nDébut du split du fichier Input");
		int startTime = (int) System.currentTimeMillis();
		
		/* Ouverture du fichier input et récupération de ses lignes */
		File inputFile = new File(nomFichier);
		List<String> lines = FileUtils.readLines(inputFile, new String("UTF-8"));
		
		/* Création de tous les fichiers Si */
		ArrayList<File> splitFileList = new ArrayList<File>();
		for(int i=0; i<nb_worker; i++)
		{
			FileUtils.deleteQuietly(new File(nomDossierShava+"/S" + 
		    new Integer(i).toString() ) );
			File fileTmp = new File(nomDossierShava+"/S" +
		    new Integer(i).toString() );
			splitFileList.add( fileTmp );
		}
			
		/* Ecriture de chaque fichier Si */
		for(int j = 0; j<nb_worker; j++)
		{
			String currentText = StringUtils.join(lines.subList(j*lines.size()/nb_worker, 
													(j+1)*lines.size()/nb_worker), "\n");
			
			FileUtils.write(splitFileList.get(j), currentText, true);
		}
		
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du split du fichier Input => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
	}
	
	
	/**
     * Split du fichier en entrée.
     * @param nbLignes
     * ce paramètre correspond aux nombres de ligne que l'on veut splitter.
     */
	
	public void lauchSlaveModeSxUMx(String dossierSlave) throws IOException
	{
		
		this.setDicoUmHost();		
		System.out.println("\nDébut du mapping en UM");
		int startTime = (int) System.currentTimeMillis();
		
		for(int j = 0; j<nb_worker; j++)
		{
			String machine = dicoUM_machine.get("UM"+new Integer(j).toString() );
			MasterConnectSlave msc = new MasterConnectSlave(machine,1000, j, dossierSlave,"modeSxUMx",NomFichierBin, user);
		    msc.run();
		}
	
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du mapping en UM => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
	}
	
	
	
	public void lauchSlaveModeUMxSMx(String dossierSlave) throws IOException
	{
				
		System.out.println("\nDébut du mapping en RSM");
		int startTime = (int) System.currentTimeMillis();
		

		
		for(String word: dicoWord_Um.keySet())
		{
			for(Integer numeroRM: dicoMachine_Words.keySet())
			{
				ArrayList<String> listeWord = dicoMachine_Words.get(numeroRM);
				    
				    for (int i=0; i<listeWord.size();i++){
				    	if(word==listeWord.get(i)){
				    		
				    		ArrayList<String> listeUM = dicoWord_Um.get(word);
				    		this.createFilefromArray(listeUM);
				    	
				    		
				    		String typeFonction = "modeUMxSMx";
				    		String machine = dicoRMS_Machine.get("RM"+new Integer(numeroRM.toString()));
				    	
				    		MasterConnectSlave msc = new MasterConnectSlave(machine,1000, numeroRM,dossierSlave,typeFonction,NomFichierBin,word, user);
						    msc.run();
				    	}
				    	
				    }
				
		     
				}

		
		}
			
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du mapping en RSM => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
	
	
	}
	
	
	public void reduceFinal() throws IOException{


		/* Création du fichier qui va contenir le résultat final */

		String resFilename = this.nomDossierShava +"/Resultat";

		File file = new File(resFilename);

		// if file doesnt exists, then create it

		 if (!file.exists()) {

		      file.createNewFile();

		    }


		 for(Integer numeroRM: dicoMachine_Words.keySet())

		{

		
		File RMSFILE = new File(this.nomDossierShava+"/RMS" + new Integer(numeroRM).toString());
		if (RMSFILE.exists()) {
		   List<String> lines = FileUtils.readLines(RMSFILE, new String("UTF-8"));
		/* Set qui va stocker les clés rencontrées */ 
		   ArrayList<String> keys = new ArrayList<String>();
		/* Boucle sur les lignes du fichier */

		for(String line : lines)

		{

		FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);

		BufferedWriter bw = new BufferedWriter(fw);


		bw.write(line);

		bw.newLine();

		bw.close();

		keys.add(line);

		}

	}


}



		}

	
	
	/**
	 * Main entry point.
	 *
	 * @param args
	 *            mettre les arguments
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	
	public static void main(String[] args) throws IOException {
		
		
		String nomInput = args[0];
		String nomDossierSlave = args[1];
		String nomDossier = args[1];
		String listeMachine = args[2];
		int nb_worker = new Integer(args[3]);
		int nb_reducer = new Integer(args[4]);
		String user = args[5];
		String nomDossierBin = args[6];
		
		
		//Pour le test
		/*
		String nomInput = "forestier_mayotteClean.txt";
		String nomDossier = "/cal/homes/mkalantari/Shavadoop_files";	
		String nomDossierSlave = "/cal/homes/mkalantari/Shavadoop_files";
		String listeMachine = "liste_machines.txt";
		String nomDossierBin = "/cal/homes/mkalantari/SHAVA/bin";
		
		int nb_worker = 4;
		int nb_reducer = 3;
		String user = "mkalantari";
		*/
		
		Master master = new Master(nb_worker,nb_reducer,nomInput,nomDossier,nomDossierBin,user);
		master.initaliseDossierShava(nomDossier);
		master.splitFichier(1);
		master.setMachine(listeMachine);
		master.lauchSlaveModeSxUMx(nomDossierSlave);
		
		master.setDicoUmHost();
		master.setDicoWordUM();
		master.setdicoRMS_Machine();
		master.setdicoMachine_Words();
		master.lauchSlaveModeUMxSMx(nomDossierSlave);
		master.reduceFinal();
		
		
		System.out.println("Tout est fini");

	} // main

} // class

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class Slave {
	
	private String nomDossier;
	private int Sx;
	private String typefonction;
	private String word;
	
	
	
	private ArrayBlockingQueue<String> standard_output = new ArrayBlockingQueue<String>(1000);
	private ArrayBlockingQueue<String> error_output = new ArrayBlockingQueue<String>(1000);
	

	
/**
     * Constructeur
     *  @param Sx : le fichier split numero x
     *  @param nomDossier : le nom de dossier ou se trouve les splits
     *  @param typeFonction : le mode de fonctionnement : modeSxUMx ou  modeUMxSMx
     *  @param word : le mot sur lequel porte la phase reduce. On peut laisser vide ce paramètre dans le mode
     *  modeSxUMx.
     */
	
	
	
	public Slave(int Sx, String nomDossier, String typeFonction, String word){
		this.Sx = Sx;
		this.nomDossier = nomDossier;
		this.typefonction = typeFonction;
		this.word = word;
	}
	

	
	/**
     * Constructeur
     *  @param Sx : le fichier split numero x
     *  @param nomDossier : le nom de dossier ou se trouve les splits
     *  @param typeFonction : le mode de fonctionnement : modeSxUMx ou  modeUMxSMx
     */

	public Slave(int Sx, String nomDossier, String typeFonction){
		this.Sx = Sx;
		this.nomDossier = nomDossier;
		this.typefonction = typeFonction;
		this.word = null;
	}

	
	private ArrayList<String> readFiletemp() throws IOException
	{
		ArrayList<String> listUM = new ArrayList<String>();
		String nomFichier = this.nomDossier +"/temp";
		
		File inputFile = new File(nomFichier);
		List<String> lines = FileUtils.readLines(inputFile, new String("UTF-8"));
		for (int i =0; i<lines.size(); i++){
			listUM.add(lines.get(i));
		}
		
		return listUM;
		
	}
	
	public void affiche(String texte){
		//System.out.println("[TestConnectionSSH "+machine+"] "+texte);
	}
	
/**
     * Méthode qui effectue le mapping Sx en Umx
     */
	
	
	public void splitmapping() throws IOException{
		
		/* Ouverture de son fichier split et récupération de ses lignes */
		File splitFile = new File(this.nomDossier+"/S" + new Integer(Sx).toString());
		//System.out.println(splitFile);
		
		List<String> lines = FileUtils.readLines(splitFile, new String("UTF-8"));
		
		/* Création du fichier qui va contenir les clés */
		String keysFilename = this.nomDossier +"/Keys"+ new Integer(Sx).toString();
		FileUtils.deleteQuietly( new File(keysFilename) );
		File keysFile = new File(keysFilename);
		
		
		/* Set qui va stocker les clés rencontrées */
		HashSet<String> keys = new HashSet<String>();
		ArrayList<String>linesOfOutputFiles = new ArrayList<String>();

		/* Boucle sur les lignes du fichier */
		for(String line : lines)
		{
			/* Pour chaque mot de la ligne on indique dans le UM qu'il est présent */
			
			String[] words = line.split(" ");
			for(String word : words)
			{
				linesOfOutputFiles.add(word + " 1");
				keys.add(word);
				//System.out.println(word);
			}		
		}
		
		/* Création des fichiers UM relatif */

			String umFilename = this.nomDossier +"/UM" + new Integer(Sx).toString();
			
			FileUtils.deleteQuietly(new File(umFilename));
			File umFile = new File(umFilename);
			String outputText = StringUtils.join(linesOfOutputFiles, "\n");
			FileUtils.write(umFile, outputText);
			
			/* Ecriture des clés */

		String keysText = StringUtils.join(keys, "\n");
		FileUtils.write(keysFile, keysText);
			
	}
	
	
/**
     * Méthode qui effectue le Suffle et le Reduce
     */
	
	public void mappingSM() throws IOException{
		
		HashMap<String, Integer> RSM = new HashMap<String, Integer>();
		ArrayList<String>linesOfOutputFiles = new ArrayList<String>();
		int count = 0;
		
		ArrayList<String>listUM = readFiletemp();
		
		for (int i=0; i<listUM.size(); i++)
		{
			String dossierUM = this.nomDossier +"/" + listUM.get(i);
    
			/* Ouverture et récupération des lignes */
		    
			File UMFile = new File(dossierUM);
			List<String> lines = FileUtils.readLines(UMFile, new String("UTF-8"));
			for(String line : lines)
			{
				
				
				String[] words = line.split(" ");
				
                if (words[0].equals(this.word)){
                  count = count + 1;
                }		
                
			}
		
		}
		
        RSM.put(word,count);
        
		linesOfOutputFiles.add(word + " "+ count);	
		
		String umFilename = this.nomDossier +"/RMS" + new Integer(Sx).toString();
		File file = new File(umFilename);
		// if file doesnt exists, then create it
					if (!file.exists()) {
						file.createNewFile();
					}
					String temp = word + " "+ new Integer(count).toString();
					FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(temp);
					bw.newLine();
					bw.close();

		
		}
		
/**
     * Méthode qui en fonction du mode de fonctinnement, va lancer le mapping 
     * Sx--> Umx si le mode est "modeSxUMx", et le mapping Umx--> RMSx si 
     * le mode est "modeUMxSMx"
     */
	
   public void typeMapping() throws IOException{
	
	  if (this.typefonction.equals("modeSxUMx") ) {
		  this.splitmapping();
	  }
	  else if (this.typefonction.equals("modeUMxSMx")) { 
		  this.mappingSM();
	  }
   }


	
	
public static void main(String[] args) throws IOException {
		
	    int nb_worker = Integer.parseInt(args[0]);
	    String fichier = args[1];
	    String typeFonction = args[2];
		String word= args[3];
	    
		if (word != null){
		Slave slave = new Slave(nb_worker,fichier,typeFonction);
        slave.typeMapping();
		}
		else {
			Slave slave = new Slave(nb_worker,fichier,typeFonction, word);
	        slave.typeMapping();
			}
		
	}//main

	
			
	}//class
	
	

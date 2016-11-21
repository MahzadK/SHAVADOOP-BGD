import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MasterConnectSlave {

	private String machine;
	private int timeout;
	private ArrayBlockingQueue<String> standard_output = new ArrayBlockingQueue<String>(1000);
	private ArrayBlockingQueue<String> error_output = new ArrayBlockingQueue<String>(1000);
	private boolean connectionOK = false;
	private String nomDossierShava;
	private int numero_worker;
	private String modeFonction;
	private ArrayList<String> listeUM;
	private String word;
	private String user;
	private String NomFichierBin ;
	
	public String getMachine() {
		return machine;
	}

	public void setConnectionOK(boolean connectionOK) {
		this.connectionOK = connectionOK;
	}

	public boolean isConnectionOK() {
		return connectionOK;
	}

	public MasterConnectSlave(String machine, int timeout, int numero_w,  String nomDossierShava, 
			String modeFonction,String NomFichierBin, String user){
		
		this.machine=machine;
		this.timeout = timeout;
		this.nomDossierShava = nomDossierShava;
		this.numero_worker = numero_w;
		this.modeFonction = modeFonction;
		this.word = null;
		this.NomFichierBin = NomFichierBin;
		this.user = user;
	}
	
	public MasterConnectSlave(String machine, int timeout, int numero_w,  
			String nomDossierShava, String modeFonction,String NomFichierBin, String word, String user ){
		
		this.machine=machine;
		this.timeout = timeout;
		this.nomDossierShava = nomDossierShava;
		this.numero_worker = numero_w;
		this.modeFonction = modeFonction;
		this.NomFichierBin = NomFichierBin;
		this.word = word;
		this.user = user;
	}
	
	
	public void affiche(String texte){
		System.out.println("[TestConnectionSSH "+machine+"] "+texte);
	}
	
	public void run(){
				 
		try {
		 
         //String[] commande = {"ssh","mkalantari@"+machine, "echo OK"};
			
			
		    String[] commande = {"ssh",user+"@"+machine, "java", "-jar", NomFichierBin+"/"+"slave.jar " +
		    					new Integer(this.numero_worker).toString() + " " +
		    					this.nomDossierShava+ " "+ this.modeFonction+" "+ this.word} ;
		    
		  
            ProcessBuilder pb = new ProcessBuilder(commande);
            Process p = pb.start();
            LecteurFlux fluxSortie = new LecteurFlux(p.getInputStream(), standard_output);
            LecteurFlux fluxErreur = new LecteurFlux(p.getErrorStream(), error_output);

            new Thread(fluxSortie).start();
            new Thread(fluxErreur).start();

            String s = standard_output.poll(timeout, TimeUnit.SECONDS);
            while(s!=null && !s.equals("ENDOFTHREAD")){
            	affiche(s);
            	if(s.contains("OK")){
            		connectionOK = true;
            	}
            	s = standard_output.poll(timeout, TimeUnit.SECONDS);
            }
            
            s = null;
            s = error_output.poll(timeout, TimeUnit.SECONDS);
            while(s!=null && !s.equals("ENDOFTHREAD")){
            	affiche(s);
            	s = error_output.poll(timeout, TimeUnit.SECONDS);
            }
         
            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        
				        
					 }
			 
			 

			 

 
	
}//class

# SHAVADOOP-BGD

Lauch master.jar with the arguments below:
• Input.txt: input to our wordcount program.
• Folder’s name: folder where we want to store the temporary files necessary to
preform the wordcount and the final result file.
• List of Telecom Paristech machines’IP.
• Number of workers that we want to use.
• Number of reducers that we want to use.
• Username : prior to launching the program, the user will have to ensure that he
has a public and a private key to use SSH protocol.
• Folder that contain the .jar files.


java -jar master.jar forestier_mayotteClean.txt /cal/homes/mkalantari/Shavadoop_files liste_machines.txt 4 3 mkalantari /cal/homes/mkalantari/SHAVA/bin

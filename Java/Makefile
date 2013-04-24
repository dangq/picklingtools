
JAVAPLACE = /home/rts/Pickling/PicklingTools150_JAVA_RESTRUCT_PROBLEMS/Java/



build :
	(cd com/picklingtools/pythonesque && make)
	(cd com/picklingtools/pickle && make)
	(javac -cp "${JAVAPLACE}" com/picklingtools/midas/MidasSocket_.java com/picklingtools/midas/MidasTalker.java com/picklingtools/midas/MidasTalker_ex.java)

midastalker_ex2 : build
	(javac -cp "${JAVAPLACE}" com/picklingtools/midas/MidasSocket_.java com/picklingtools/midas/MidasTalker.java com/picklingtools/midas/MidasTalker_ex2.java)

run : build
	java -cp "${JAVAPLACE}" com.picklingtools.midas.MidasTalker_ex




clean :
	(cd com/picklingtools/pythonesque && make clean)
	(cd com/picklingtools/pickle && make clean)
	(cd com/picklingtools/midas && make clean)


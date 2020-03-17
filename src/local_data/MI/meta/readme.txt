2018 Michigan Precinct-Level General Election Results files

The file 2018XXX.zip unzips into five files: 2018name.txt, 2018offc.txt,
2018vote.txt, 2018city.txt & county.txt. Once unzipped these files will 
take up approx. 20meg of hard disk space. The files are TAB-delimited. 

The file layout for the names file is as follows:

Field#1:  Election Year	(PK)	Numeric
Field#2:  Election Type	(PK)	Text maxlen=3 (PRI=Primary,GEN=General)
Field#3:  Office Code	(PK)	Numeric

           1 President of the United States
           2 Governor
           3 Secretary of State
           4 Attorney General
           5 United States Senator
           6 U.S. Representative in Congress
           7 State Senator
           8 State Representative
           9 Member of the State Board of Education
          10 Member of the University of Michigan Board of Regents
          11 Member of the Michigan State University Board of Trustees
          12 Member of the Wayne State University Board of Governors
          13 Justice of the Supreme Court
          90 Statewide Ballot Proposals
		 
Field#4:  District Code	(PK)	Text maxlen=5 (Make sure imports as Text!)

		  Reference Office Description in offices file.

Field#5:  Status Code	(PK)	Numeric (Included in offices file)

          0    Regular Term
          1    Non-Incumbent
          2-4  Incumbent - Partial Term
          5-7  Non-Incumbent - Partial Term
          8    Partial Term
          9-10 New Judgeship

Field#6:  Candidate ID#	(PK)	Numeric (includes some negative numbers)
Field#7:  Candidate Last Name	Text maxlen=40
Field#8:  Candidate First Name	Text maxlen=32
Field#9:  Candidate Middle Name	Text maxlen=32
Field#10: Candidate Party Name	Text maxlen=5 (new field in 2008)

          DEM        Democratic Party
          GRN        Green Party
          LIB        Libertarian Party
          NLP        Natural Law Party
          NPA        No Party Affiliation
          REP        Republican Party
          RFP        Reform Party
          TIS        TIS
          UST        US Taxpayers
          WORW       Workers World

The file layout for the offices file is as follows:

Field#1:  Election Year	(PK)	See above*
Field#2:  Election Type	(PK)	See above*
Field#3:  Office Code	(PK)	See above*
Field#4:  District Code	(PK)	See above* (Make sure imports as Text!)
Field#5:  Status Code	(PK)	See above*
Field#6:  Office Description 	Text maxlen=255

          Detailed description of the combination of the Office Code,
          District Code and Status Code fields, including the number
          of open positions within the office and where the candidate
          may have filed.

* Fields shared between offices and other files. Used to "join" or 
  link information together.

The file layout for the votes file is as follows:

Field#1:  Election Year	 (PK)See above*
Field#2:  Election Type	 (PK)See above*
Field#3:  Office Code	 (PK)See above*
Field#4:  District Code	 (PK)See above* (Make sure imports as Text!)
Field#5:  Status Code	 (PK)See above*
Field#6:  Candidate ID#	 (PK)See above*
Field#7:  County Code	 (PK)Numeric (See county.txt)
Field#8:  City/Town Code (PK)Numeric (See cities file)
Field#9:  Ward Number	 (PK)Numeric (Zero-filled if not applicable)
Field#10: Precinct Number(PK)Numeric (>=900=Absent Voter Counting Board)
Field#11: Precinct Label (PK)Text maxlen=10 (AVCB=Absent Voter Counting Board)
Field#12: Precinct #Votes    Numeric (Poll Book Totals if Office Code=0)

* Fields shared between both names and votes files. Used to "join" or 
  link information together.

The file layout for the cities file is as follows:

Field#1:  Election Year	(PK)		See above*
Field#2:  Election Type	(PK)		See above*
Field#3:  County Code	(PK)		See above*
Field#4:  City/Township Code(PK)	See above*
Field#5:  City/Township Description	Text maxlen=50

* Fields shared between cities and other files. Used to "join" or 
  link information together.

The file layout for the county file is as follows:

Field#1   County Code	(PK)	See above*
Field#2   County Name			Text maxlen=64

* Fields shared between county and other files. Used to "join" or 
  link information together.

Hints & Tips

(PK) Primary Keys are unique by definition and not absolutely necessary
to obtain what you want from this data. They are only suggestions.

You may choose to ignore/eliminate the Election Year/Type fields
throughout if you find them redundant, however, if you do not eliminate
them, (or at least one of them) upon import of the votes table, the 
suggested Primary Key will not work in products such as Access that 
limit the number of fields in a Primary Key to ten.
 
Two composite indexes are suggested for the votes table, regardless of
whether or not you choose to use Primary Keys:

Key#1: Election Year,
       Election Type,
       Office Code,
       District Code,
       Status Code,
       Candidate ID#
	
Key#2: Election Year,
       Election Type,
       County Code,
       City/Town Code

Eliminate any fields from the above keys if you have chosen not to import them.

Any questions regarding this information can be directed to the Michigan
Bureau of Elections by calling (517) 335-3234.

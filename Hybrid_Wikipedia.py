import FullPhase as fp
#import Embedding as emb
import pandas as pd
import time

import csv
import json
import os
import glob

import LabelColumn as LC
import MetricsCalcul_Wikipedia as MCal
import PrecisionRecallF1 as PRF
def RunHybrid(TableList):
	print("I am in Hybrid")
	#---------------------------------- Defining the table ----------------------------------
	#T = pd.DataFrame({'Country': ['Dominica','Tajikistan','Djibouti','Gabon'],
	#            'Population': [1000,2000,3000,4000],
	#            'Capital': ['Roseau','Dushanbe','Djibouti','Libreville']})

	#T = pd.DataFrame({'Country': ['Ghana','Tajikistan','Djibouti','Senegal'],
	#		'Population': [1000,2000,3000,4000],
	#		'Capital': ['Accra','Dushanbe','Djibouti','Dakar ']})

	#-----------------------------------------------------------------------------------------------------------
	#					single table test
	#-----------------------------------------------------------------------------------------------------------
	# T = pd.read_csv("./data/T2D/t2d_tables_instance/9475172_1_1023126399856690702.csv")
	# table_csv = "9475172_1_1023126399856690702.csv"
	#-----------------------------------------------------------------------------------------------------------


	#reading the Wikipedia table
	os.chdir('./data/Wikipedia/tables_instance/csv/')

	#contains prediction results
	mapped_Prediction_Dict = dict()
	TP_Total = 0
	FN_Total = 0
	FP_Total = 0
	precision = 0
	recall = 0 
	F1 = 0
	#change the path to  have csv filename without the path inside
	os.chdir('/home/yasamin/Codes/WebTableAnnotation/data/Wikipedia/tables_instance/csv/')

	#reading all CSV files in path
	allCsvTableFiles = glob.glob('*.csv')


	#print(allCsvTableFiles)
	start_time = time.time()
	T = pd.DataFrame()
	E = pd.DataFrame()
	i = 0 
	#Tables_list2 = ['71840765_0_6664391841933033844.csv', '49772588_0_6549847739640234347.csv', '68779923_1_3240042497463101224.csv', '88353875_0_4876234304797064286.csv', '55961337_0_6548713781034932742.csv', '8009320_0_5507755752653817624.csv', '39173938_0_7916056990138658530.csv', '28079336_1_3124145965038277571.csv', '79327346_0_5787863739697336601.csv', '24859353_0_7027810986004269522.csv', '73988811_0_2775758476756716904.csv', '11599512_1_280388135214354946.csv', '69881946_0_1105130426898457358.csv', '6869358_0_1379459120563510331.csv', '38732532_0_1420803645394505878.csv', '33401079_0_9127583903019856402.csv', '29021592_4_4348860179678308511.csv', '29414811_6_8221428333921653560.csv', '11688006_0_8123036130090004213.csv', '13719111_1_5719401842463579519.csv', '26270372_1_3774607938887520564.csv', '57938705_0_8737506792349461963.csv', '60319454_0_3938426910282115527.csv', '19272019_0_1508498249156534553.csv', '35188621_0_6058553107571275232.csv', '13395751_3_369722706637560735.csv', '63389809_0_8179819543692215824.csv', '64207896_0_3206705146602325658.csv', '39759273_0_1427898308030295194.csv', '70515524_0_5425995199745290007.csv', '4730513_0_8573199053866072336.csv', '41194422_0_7231546114369966811.csv', '61121469_0_6337620713408906340.csv', '21362676_0_6854186738074119688.csv', '52299421_0_4473286348258170200.csv', '14067031_0_559833072073397908.csv', '49801939_0_6964113429298874283.csv', '3887681_0_7938589465814037992.csv', '33242247_1_7232384061498591611.csv', '21329809_0_5526008408364682899.csv', '29414811_12_251152470253168163.csv', '12125836_0_1134348206297032434.csv', '38428277_0_1311643810102462607.csv', '69381058_0_984708047398578866.csv', '96203994_0_2127964719640427252.csv', '37815699_0_8391788584341609964.csv']
	#first list : ['4501311_8_8306082458935575308.csv', '21333456_2_1886495893795687264.csv', '5972322_0_3150366936082777235.csv', '61626303_0_266372209664087642.csv', '12271141_0_8517913935669973086.csv', '28646774_0_3256889721737611537.csv', '55004961_0_2904467548072189860.csv', '16767252_0_2409448375013995751.csv', '90196673_0_5458330029110291950.csv', '62564020_0_3836030043284699244.csv', '89511064_0_2199624509082573904.csv', '69537082_0_7789694313271016902.csv', '14380604_4_3329235705746762392.csv', '21801207_0_8144839668470123042.csv', '2066887_0_746204117268168398.csv', '99070098_0_2074872741302696997.csv', '41480166_0_6681239260286218499.csv', '43226272_0_5995103035300388365.csv', '42764224_0_515744189130384109.csv', '97941125_0_8220652154649529701.csv', '29857217_0_961038783966485949.csv', '32812626_0_5428724146873158593.csv', '19361188_0_1640726405141876003.csv', '25933880_0_4058554985574416690.csv', '48805028_0_8933391169600128370.csv', '21585935_0_294037497010176843.csv', '66009064_0_9148652238372261251.csv', '45073662_0_3179937335063201739.csv', '28086084_0_3127660530989916727.csv', '53822652_0_5767892317858575530.csv', '64734047_0_5880705307495695536.csv', '21337553_0_8832378999628437599.csv', '63450419_0_8012592961815711786.csv', '92896255_0_4593968708343548527.csv']
	for table_csv in allCsvTableFiles:
		#i = i + 1
		if(table_csv in TableList):
			os.chdir('/home/yasamin/Codes/WebTableAnnotation/data/Wikipedia/tables_instance/csv/')
			with open(table_csv, 'r',encoding='utf-8') as csvTableFile:
				print("This is the Table file name :\n\n",table_csv)
				try:
					#although we have header,...!
					T = pd.read_csv(table_csv, index_col=False, header=None)
				except pd.errors.ParserError:
					#variant column numbers in different rows
					print("variant number of columns handled!")
					with open(table_csv, 'r') as f:
						for line in f:
							T = pd.concat( [T, pd.DataFrame([tuple(line.strip().split(','))])], ignore_index=True )        
							
				#switch path to normal form
				os.chdir('/home/yasamin/Codes/WebTableAnnotation/')
				

				#Remove the row if there is no GT in entity file for it.
				entity_csv = table_csv
				os.chdir('/home/yasamin/Codes/WebTableAnnotation/data/Wikipedia/entities_instance/csv/')
				with open(entity_csv, 'r',encoding='utf-8') as csvEntityFile:
					print("This is the Entity file name :\n\n",entity_csv)
					try:
						E = pd.read_csv(entity_csv, index_col=False, header=None)
					except pd.errors.ParserError:
						#variant column numbers in different rows
						print("variant numexit in Table handled!")
						with open(entity_csv, 'r') as f:
							for line in f:
								E = pd.concat( [E, pd.DataFrame([tuple(line.strip().split(','))])], ignore_index=True )        
					
					count_row = T.shape[0]
					for i, row in T.iterrows():
						if not((i) in E[E.columns[-1]].values):
							print("This i does not exists:",i)
							try:
								#drop the row
								T = T.drop([i])										
							except Exception:
								print("The T was this when the exception happened:")
								print("T:",T)
						
				#switch path to normal form
				os.chdir('/home/yasamin/Codes/WebTableAnnotation/')

			#----------------------------------------------------------------
								#uncomment for Hybrid I
			#----------------------------------------------------------------
			#Hybrid1
				if(T.empty or E.empty):
					print("table name:",table_csv,"is empty, we skip!")
					continue

				TPrimeIsAnnotated , TPrimeAnnotation = fp.getFullPhase(T,table_csv)
				# EmbTPrimeIsAnnotated, EmbTPrimeAnnotation = emb.getEmbedding(T)
				# for k in TPrimeIsAnnotated:
				# 	if(TPrimeIsAnnotated[k] == True):
				# 		continue
				# 	elif(TPrimeIsAnnotated[k] == False):
				# 		TPrimeIsAnnotated[k] = EmbTPrimeIsAnnotated[k]
				# 		TPrimeAnnotation[k] = EmbTPrimeAnnotation[k]
				
				# print("Hybrid I result:")
				# print("-"*30)
				# print(TPrimeAnnotation)
				# print(TPrimeIsAnnotated)

			#----------------------------------------------------------------
								#uncomment for Hybrid II
			#----------------------------------------------------------------
			#Hybrid2
				# TPrimeIsAnnotated, TPrimeAnnotation = emb.getEmbedding(T)
				# FPTPrimeIsAnnotated, FPTPrimeAnnotation = fp.getFullPhase(T,table_csv)
				# for k in TPrimeIsAnnotated:
				# 	if(TPrimeIsAnnotated[k] == True):
				# 		continue
				# 	elif(TPrimeIsAnnotated[k] == False):
				# 		TPrimeIsAnnotated[k] = FPTPrimeIsAnnotated[k]
				# 		TPrimeAnnotation[k] = FPTPrimeAnnotation[k]
				
				# print("Hybrid II result:")
				# print("-"*30)
				# print(TPrimeAnnotation)
				# print(TPrimeIsAnnotated)
			#----------------------------------------------------------------

				#metrics:
				TP, FN, FP =  MCal.MetricsCalcul(T,table_csv,TPrimeAnnotation,TPrimeIsAnnotated)
				TP_Total = TP_Total+TP
				FN_Total = FN_Total+FN
				FP_Total = FP_Total+FP
				precision, recall, F1 = PRF.Precision_Recall_F1(TP_Total,FN_Total,FP_Total)
				#if(i == 10):
				
				
			#Hybrid2
				#print("Embedding part started.")

				#EmbTPrimeIsAnnotated, EmbTPrimeAnnotation = emb.getEmbedding(T)

				
	#------------------------------------------------------------------------------------------
	print("Final result of the core partition")
	print("-"*30)
	print("Precision : ",precision)
	print("Recall : ",recall)
	print("F1 : ",F1)
	print("-"*30)
	print("FN ",FN_Total)
	print("FP ",FP_Total)
	print("TP ",TP_Total)
	print("-"*30)
	# print("-----------------------------------------------------------------------------------")
	# print("Run time for 1 lookup phase and one embedding phase, Local Endpoint ",time.time() - start_time)
	# print("-----------------------------------------------------------------------------------")
	return
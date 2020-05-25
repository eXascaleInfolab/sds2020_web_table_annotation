""" Creating Levenshtein and Type Pickle files only for Label Column"""

import RefinedLookup as RLU
from collections import defaultdict
import glob
import os
import pandas as pd
import pickle 
import html
import LabelColumn as LC
import multiprocessing
import time
import string
import re
import math
import Levenshtein

search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))

def GetEntityClass(T, LabelColumn):

    #dictionary - Key: entity, Value: list of Instance of(P31)
    InstanceOfDict = defaultdict(list)
    SFDistanceDict = {}
#open the table file
    #for each row i of T do
    for index, row in T.iterrows():
        #get label column of the row
        try:
            label = row[LabelColumn]
        except IndexError:
            print("row label column not exists")

        #preprocessing - to lower and remove spaces
        try:	
            label = label.lower()
            label = re.sub("\s\s+" , " ", label)
        except Exception:
            continue
        
        try:
            results = search[label]
            print("result is:",results,"for label:",label)
			#if we come here, it means there is something in result
			#if not, we had the exception.
            for r in results:
                print("THIS IS R:",r)
                r_type = RLU.getTypes(r)
                print("THIS IS R TYPE:",r_type)
                if(all(x in r_type for x in ['Wikimedia disambiguation page'])):
                    results.remove(r)
                    r_type.remove('Wikimedia disambiguation page')
                if(all(x in r_type for x in ['Wikimedia category'])):
                    results.remove(r)
                    r_type.remove('Wikimedia category')
                #add types of the label to Dict
                #if page has the type
                if not(len(r_type)==0):
                    if(label in InstanceOfDict):   
                        for rt in r_type:
                            InstanceOfDict[label].append(rt)
                    elif not(label =='' or label == ' '):
                        InstanceOfDict[label] = r_type
				#if the only result was disambiguation page	and removed:
            if(len(results)==0):
                raise KeyError('we had just Wikimedia disambiguation pages. now result is zero.')

			#results= search.SearchWithDistanceSF(label)
        except KeyError:
            print("keyword ",label, "not in Surface form")
			#put the key in the dict to check it again when levenshtein 
			#when u know the acceptable types.
			
            try:
                levenshtein_candidate = []
                for i in search:
                    similarityRatio = Levenshtein.ratio(i,label)
                    if(similarityRatio>0.80):
                        if(i in levenshtein_candidate):
                            for j in search[i]:
                                levenshtein_candidate.append(j)
                        else:
                            levenshtein_candidate = search[i]
                levenshtein_candidate.sort()
                if not(levenshtein_candidate == []):
                    #TODO: take all levenshtein candidates instead of firsr 
                    results = levenshtein_candidate
                    SFDistanceDict[label] = levenshtein_candidate
                    print("solved with levenshtein")
                    print("label:",label)
                    print("solved result",results)
                    #remove wikimedia disambiguation after the levenshtein
                    for r in results:
                        print("THIS IS R:",r)
                        r_type = RLU.getTypes(r)
                        print("THIS IS R TYPE:",r_type)
                        if(all(x in r_type for x in ['Wikimedia disambiguation page'])):
                            results.remove(r)
                        if(all(x in r_type for x in ['Wikimedia category'])):
                            results.remove(r)
                        #add types of the label to Dict
                        #if page has the type
                        if not(len(r_type)==0):
                            if(label in InstanceOfDict):   
                                for rt in r_type:
                                    InstanceOfDict[label].append(rt)
                            elif not(label =='' or label == ' '):
                                InstanceOfDict[label] = r_type
            except IndexError:
                print("no result from levenshtein on SF")
                continue
            continue
    
		# #remove wikimedia disambiguation after the levenshtein
        # for r in results:
        #     r_type = RLU.getTypes(r)
        #     if(all(x in r_type for x in ['Wikimedia disambiguation page'])):
        #         results.remove(r)
        #     if(all(x in r_type for x in ['Wikimedia category'])):
        #         results.remove(r)
        #     #add types of the label to Dict
        #     if(label in InstanceOfDict):   
        #         for rt in r_type:
        #             InstanceOfDict[label].append(rt)
        #     elif not(label =='' or label == ' '):
        #         InstanceOfDict[label] = r_type


    print("SFDistanceDict:",SFDistanceDict)
    print("InstanceOfDict:",InstanceOfDict)

    return(SFDistanceDict,InstanceOfDict)



def Preprocess(T):

    #remove punctuations and html characters reference from tables
	for col in T:
		Keep_Column = []
		for row in T[col]:
			try:
				row = "".join([ch for ch in row if ch not in string.punctuation])
				Keep_Column.append(row)
			except TypeError:
				print("cannot remove punctuations")
				Keep_Column.append(row)
				continue
		T[col] = Keep_Column


    #removing numeric datatypes
	T = T.select_dtypes(exclude=['int_','float_','complex_'])

	for col in T:
		try:
			#remove if it is only digits (no letter or other inputs)
			for i, v in T[col].items():			  
				T[col][i] = re.sub('^[0-9 ]+$','',v)

		except TypeError:
			print("cannot replace numeric values")
			continue


	for col in T:
		#print("This is T[col] before replace html ref", T[col])
		try:
            #remove NaN - string type
			for i, v in T[col].items():			  
				if(math.isnan(v) ):
					T[col][i] = ""

			#T[col] = [elem for elem in T[col] if not math.isnan(elem)]		
            
            #replace html characters reference in tables
			T[col] = [html.unescape(elem) for elem in T[col]]
			#remove first spaces
			T[col] = [re.sub(r"^\s+", "", elem) for elem in T[col]]
			#remove last spaces
			T[col] = [re.sub(r"\s+$", "", elem) for elem in T[col]]
		except TypeError:
			print("Type not suitable for iteration")
			print("T[col]",T[col])
			continue


	#4.labelColumn  <- getLabelColumn(T);
	LabelColumn = LC.getLabelColumn(T)

	print("-" * 50)
	print(" " * 50)
	print("-" * 50)
    

	return(T, LabelColumn)


#read the table from Limaye
def ReadTable(TableNames):
    
    LevenshteinDict = {}
    InstanceOfDict = defaultdict(list)
    
    T = pd.DataFrame()

    for table_csv in TableNames:
        #os.chdir('./data/Limaye/tables_instance/')
        os.chdir('./data/T2D/Final_InstanceLevel_GoldStandard/tables_instance(Instance_level_GS)/')

        with open(table_csv, 'r',encoding='utf-8') as csvTableFile:
            print("This is the Table file name :\n\n",table_csv)
				
            T = pd.read_csv(table_csv, header=None)

            #switch path to normal form
            os.chdir('/home/yasamin/Codes/WebTableAnnotation/')

            #remove unwanted values and detect Label Column
            T, LabelColumn = Preprocess(T)
            #merge new results with previous "instance of" in dict
            LevenshteinTemp, InstanceOfTemp =  GetEntityClass(T, LabelColumn)
            LevenshteinDict = {**LevenshteinDict,**LevenshteinTemp}
            InstanceOfDict = {**InstanceOfDict, **InstanceOfTemp}
            

    print("LevenshteinDict",LevenshteinDict)
    print("InstanceOfDict", InstanceOfDict)
    return(LevenshteinDict,InstanceOfDict)




# multiprocessing to send table names
def Multiprocess_Limaye():
    InstanceOfDict = defaultdict(list)
    LevenshteinDict = defaultdict(list)

    List1 = ['file430636_0_cols1_rows9.csv', 'file539385_17_cols1_rows87.csv','file422017_0_cols1_rows12.csv', 'file236160_0_cols1_rows28.csv', 'file505656_0_cols1_rows20.csv', 'file398415_0_cols1_rows58.csv', 'file516163_0_cols1_rows24.csv', 'file525008_0_cols1_rows9.csv', 'file139009_8_cols1_rows21.csv', 'file313602_0_cols1_rows78.csv', 'file290427_0_cols1_rows14.csv', 'file266838_5_cols1_rows22.csv', 'file505202_0_cols1_rows6.csv', 'file358104_14_cols1_rows65.csv', 'file199969_2_cols1_rows19.csv', 'file222751_0_cols1_rows13.csv', 'file180087_0_cols1_rows12.csv', 'file42049_0_cols1_rows6.csv', 'file202088_0_cols1_rows32.csv','file363812_1_cols1_rows21.csv'] #20
    List2 = ['file183833_0_cols1_rows21.csv', 'file411836_0_cols1_rows83.csv', 'file593609_0_cols1_rows14.csv', 'file98470_1_cols1_rows55.csv', 'file409727_0_cols1_rows20.csv', 'file365069_0_cols1_rows66.csv', 'file40567_0_cols1_rows6.csv', 'file503004_0_cols1_rows20.csv', 'file426195_0_cols1_rows21.csv', 'file191426_0_cols1_rows31.csv', 'file496605_1_cols1_rows39.csv', 'file464588_0_cols1_rows12.csv', 'file451761_0_cols1_rows19.csv', 'file270041_0_cols1_rows23.csv', 'file431641_1_cols1_rows62.csv', 'file107572_0_cols1_rows62.csv', 'file30597_5_cols1_rows11.csv', 'file169346_0_cols1_rows7.csv', 'file461483_0_cols1_rows39.csv','file65862_24_cols1_rows58.csv'] #20 Done
    List3 = ['file21126_1_cols1_rows86.csv', 'file386878_0_cols1_rows23.csv', 'file331899_0_cols1_rows9.csv', 'file288111_0_cols1_rows34.csv', 'file152457_0_cols1_rows7.csv', 'file39042_0_cols1_rows6.csv', 'file366288_0_cols1_rows29.csv', 'file268478_11_cols1_rows25.csv', 'file282772_0_cols1_rows45.csv', 'file136273_4_cols1_rows7.csv', 'file138986_23_cols1_rows17.csv', 'file429559_0_cols1_rows31.csv', 'file413846_0_cols1_rows17.csv', 'file268478_10_cols1_rows25.csv', 'file13997_0_cols1_rows20.csv', 'file71478_0_cols1_rows17.csv', 'file417489_17_cols1_rows28.csv', 'file439313_0_cols1_rows68.csv', 'file533779_0_cols1_rows7.csv','file160562_0_cols1_rows11.csv'] #20
    List4 = ['file16426_0_cols1_rows11.csv', 'file239528_0_cols1_rows39.csv', 'file184323_0_cols1_rows67.csv', 'file181303_0_cols1_rows31.csv', 'file34615_2_cols1_rows24.csv', 'file315351_0_cols1_rows14.csv', 'file273143_0_cols1_rows6.csv', 'file391005_0_cols1_rows9.csv', 'file397636_0_cols1_rows14.csv', 'file812_0_cols1_rows20.csv', 'file110816_0_cols1_rows40.csv', 'file405469_0_cols1_rows16.csv', 'file286121_14_cols1_rows65.csv', 'file234558_0_cols1_rows39.csv', 'file333289_2_cols1_rows12.csv', 'file202151_0_cols1_rows31.csv', 'file440470_0_cols1_rows26.csv', 'file429503_0_cols1_rows12.csv', 'file57149_0_cols1_rows12.csv','file224977_0_cols1_rows66.csv']#20 Done
    List5 = ['file234482_0_cols1_rows68.csv', 'file420753_0_cols1_rows11.csv', 'file234296_0_cols1_rows14.csv', 'file68383_1_cols1_rows60.csv', 'file385293_0_cols1_rows14.csv', 'file521391_0_cols1_rows6.csv', 'file73526_0_cols1_rows46.csv', 'file95757_0_cols1_rows19.csv', 'file191712_11_cols1_rows25.csv', 'file291850_0_cols1_rows9.csv', 'file380717_0_cols1_rows8.csv', 'file187184_0_cols1_rows84.csv', 'file268478_8_cols1_rows20.csv', 'file40637_0_cols1_rows12.csv', 'file199463_1_cols1_rows14.csv', 'file100415_0_cols1_rows14.csv', 'file489155_0_cols1_rows17.csv', 'file257923_0_cols1_rows84.csv', 'file2503_2_cols1_rows12.csv','file180098_0_cols1_rows23.csv'] #20
    List6 = ['file82769_0_cols1_rows83.csv' , 'file37584_2_cols1_rows28.csv', 'file242908_0_cols1_rows9.csv', 'file240410_0_cols1_rows60.csv', 'file217042_0_cols1_rows14.csv', 'file531575_0_cols1_rows31.csv', 'file192085_0_cols1_rows31.csv', 'file258799_0_cols1_rows40.csv', 'file501300_8_cols1_rows35.csv', 'file532025_0_cols1_rows11.csv', 'file60228_9_cols1_rows37.csv', 'file569522_0_cols1_rows21.csv', 'file592476_0_cols1_rows8.csv', 'file558362_0_cols1_rows20.csv', 'file405886_0_cols1_rows14.csv', 'file117573_0_cols1_rows23.csv', 'file362299_0_cols1_rows9.csv', 'file421553_0_cols1_rows14.csv', 'file119797_0_cols1_rows39.csv','file27532_1_cols1_rows53.csv'] #20 Done
    List7 = ['file413245_3_cols1_rows12.csv', 'file494624_0_cols1_rows35.csv', 'file77529_0_cols1_rows20.csv', 'file51375_4_cols1_rows88.csv', 'file325689_0_cols1_rows11.csv', 'file267997_0_cols1_rows69.csv', 'file30066_0_cols1_rows14.csv', 'file139009_9_cols1_rows21.csv', 'file289661_0_cols1_rows6.csv', 'file416640_0_cols1_rows6.csv', 'file434605_0_cols1_rows15.csv', 'file405599_0_cols1_rows23.csv', 'file138978_0_cols1_rows83.csv', 'file221797_1_cols1_rows39.csv', 'file99290_0_cols1_rows79.csv', 'file244317_0_cols1_rows39.csv', 'file167874_0_cols1_rows11.csv', 'file65785_1_cols1_rows57.csv', 'file183712_0_cols1_rows19.csv','file178948_0_cols1_rows14.csv'] #19
    List8 = ['file456872_2_cols1_rows15.csv', 'file91274_1_cols1_rows17.csv', 'file38336_0_cols1_rows12.csv', 'file383362_0_cols1_rows21.csv', 'file8429_0_cols1_rows83.csv', 'file236612_0_cols1_rows14.csv', 'file41945_0_cols1_rows87.csv', 'file124647_0_cols1_rows9.csv', 'file449225_0_cols1_rows50.csv', 'file212038_0_cols1_rows9.csv', 'file184372_0_cols1_rows13.csv', 'file525125_0_cols1_rows39.csv', 'file377321_1_cols1_rows24.csv', 'file51190_1_cols1_rows6.csv', 'file233008_0_cols1_rows39.csv', 'file466379_0_cols1_rows11.csv', 'file37584_0_cols1_rows56.csv', 'file288989_0_cols1_rows68.csv', 'file420324_0_cols1_rows12.csv','file215105_0_cols1_rows23.csv'] #20
    List9 = ['file381801_0_cols1_rows6.csv' , 'file228670_0_cols1_rows20.csv', 'file409049_0_cols1_rows46.csv', 'file191825_0_cols1_rows15.csv', 'file435083_10_cols1_rows9.csv', 'file396413_0_cols1_rows9.csv', 'file304130_0_cols1_rows9.csv', 'file304250_0_cols1_rows14.csv', 'file134372_0_cols1_rows11.csv', 'file404549_0_cols1_rows20.csv', 'file306553_0_cols1_rows14.csv', 'file339527_0_cols1_rows19.csv', 'file331278_0_cols1_rows23.csv', 'file139009_10_cols1_rows25.csv', 'file219474_11_cols1_rows22.csv', 'file455767_0_cols1_rows44.csv', 'file365726_0_cols1_rows13.csv', 'file479464_0_cols1_rows14.csv','file172679_0_cols1_rows14.csv','file171157_0_cols1_rows21.csv'] #20 Done
    List10 = ['file550665_0_cols1_rows20.csv', 'file268478_9_cols1_rows20.csv', 'file344029_0_cols1_rows34.csv', 'file469366_1_cols1_rows19.csv', 'file214477_0_cols1_rows45.csv', 'file143181_0_cols1_rows56.csv', 'file85468_0_cols1_rows80.csv', 'file114403_0_cols1_rows82.csv', 'file269877_0_cols1_rows39.csv', 'file208153_0_cols1_rows17.csv', 'file493618_0_cols1_rows39.csv', 'file139009_11_cols1_rows25.csv', 'file568853_0_cols1_rows18.csv', 'file251103_0_cols1_rows19.csv', 'file359655_0_cols1_rows14.csv', 'file191712_8_cols1_rows20.csv', 'file137293_0_cols1_rows6.csv', 'file334380_0_cols1_rows21.csv','file132514_0_cols1_rows17.csv','file241396_9_cols1_rows69.csv'] #20
    List11 = ['file267184_0_cols1_rows22.csv', 'file526591_0_cols1_rows20.csv', 'file450629_12_cols1_rows24.csv', 'file177766_0_cols1_rows83.csv', 'file499936_5_cols1_rows86.csv', 'file338795_0_cols1_rows83.csv', 'file420793_0_cols1_rows12.csv', 'file235911_0_cols1_rows40.csv', 'file277954_0_cols1_rows21.csv', 'file98594_5_cols1_rows22.csv', 'file220036_5_cols1_rows15.csv', 'file110008_0_cols1_rows66.csv', 'file487374_0_cols1_rows9.csv', 'file384878_0_cols1_rows17.csv', 'file88377_0_cols1_rows13.csv', 'file135940_0_cols1_rows35.csv', 'file288293_0_cols1_rows14.csv', 'file388743_6_cols1_rows28.csv','file63591_4_cols1_rows58.csv','file506769_0_cols1_rows24.csv'] #20
    List12 = ['file191712_9_cols1_rows20.csv', 'file63247_13_cols1_rows65.csv', 'file136273_3_cols1_rows11.csv', 'file51833_0_cols1_rows68.csv', 'file226691_0_cols1_rows20.csv', 'file69673_0_cols1_rows58.csv', 'file39810_0_cols1_rows6.csv', 'file284898_0_cols1_rows31.csv', 'file539385_18_cols1_rows87.csv', 'file263556_0_cols1_rows45.csv', 'file27532_0_cols1_rows82.csv', 'file98594_6_cols1_rows25.csv', 'file568285_0_cols1_rows15.csv', 'file467640_0_cols1_rows20.csv', 'file322685_0_cols1_rows29.csv', 'file404761_0_cols1_rows11.csv', 'file32078_0_cols1_rows12.csv', 'file101640_0_cols1_rows31.csv','file338151_0_cols1_rows6.csv'] #19
    List13 = ['file137959_0_cols1_rows19.csv', 'file409309_0_cols1_rows60.csv', 'file389632_0_cols1_rows31.csv', 'file394725_0_cols1_rows24.csv', 'file99290_1_cols1_rows63.csv', 'file522774_0_cols1_rows20.csv', 'file114818_0_cols1_rows22.csv', 'file455094_0_cols1_rows9.csv', 'file444427_0_cols1_rows22.csv', 'file329942_5_cols1_rows18.csv', 'file281613_0_cols1_rows9.csv', 'file489984_0_cols1_rows86.csv', 'file64479_0_cols1_rows12.csv', 'file495772_0_cols1_rows24.csv', 'file277057_2_cols1_rows31.csv', 'file173767_1_cols1_rows33.csv', 'file322407_2_cols1_rows18.csv', 'file294473_0_cols1_rows17.csv','file162595_3_cols1_rows19.csv'] #19 Done
    List14 = ['file229323_0_cols1_rows7.csv', 'file144332_0_cols1_rows14.csv', 'file155305_0_cols1_rows17.csv', 'file331221_0_cols1_rows9.csv', 'file337276_0_cols1_rows31.csv', 'file123945_0_cols1_rows17.csv', 'file492921_0_cols1_rows39.csv', 'file270598_0_cols1_rows13.csv', 'file376591_0_cols1_rows19.csv', 'file64428_2_cols1_rows18.csv', 'file339783_0_cols1_rows11.csv', 'file399053_0_cols1_rows14.csv', 'file195533_0_cols1_rows18.csv', 'file198804_0_cols1_rows39.csv', 'file431641_0_cols1_rows78.csv', 'file191712_10_cols1_rows25.csv', 'file345145_0_cols1_rows8.csv', 'file535849_0_cols1_rows20.csv','file493682_0_cols1_rows31.csv'] #19
    List15 = ['file196742_0_cols1_rows14.csv', 'file256909_0_cols1_rows39.csv', 'file205708_0_cols1_rows20.csv', 'file499596_0_cols1_rows39.csv', 'file391510_0_cols1_rows19.csv', 'file265665_0_cols1_rows15.csv', 'file231281_1_cols1_rows39.csv', 'file437154_0_cols1_rows31.csv', 'file118301_28_cols1_rows28.csv', 'file75651_0_cols1_rows14.csv', 'file270540_4_cols1_rows17.csv', 'file285010_0_cols1_rows31.csv', 'file279754_0_cols1_rows19.csv', 'file431384_13_cols1_rows65.csv', 'file211429_0_cols1_rows92.csv', 'file219474_12_cols1_rows17.csv', 'file6170_0_cols1_rows24.csv', 'file145174_1_cols1_rows37.csv','file338795_1_cols1_rows83.csv'] #19
    TableList = [List1, List2, List3, List4, List5, List6, List7, List8, List9, List10, List11, List12, List13, List14, List15]
    
    pool = multiprocessing.Pool()
    #input list, out is list of tuple
    results = pool.map(ReadTable, TableList) #--> hamechi tu X bud.
    
    x = [result[0] for result in results]
    print("X:",x)
    for i in x:
        if not(i == {}):
            LevenshteinDict = {**LevenshteinDict,**i}

    y = [result[1] for result in results]
    print("Y:",y)
    for i in y:
        if not(i == {}):
            InstanceOfDict = {**InstanceOfDict, **i}

    print("Final Levenshtein Dict:",LevenshteinDict)
    print("Final Types Dict",InstanceOfDict)
    
    #save dictionary of label:instanceof to pickle file
    with open('data/LimayeTypes.pickle', 'wb') as handle:
        pickle.dump(InstanceOfDict, handle)
    #save dictionary of Levenshtein code to pickle file
    with open('data/LimayeLevenshtein.pickle', 'wb') as handle:
        pickle.dump(LevenshteinDict, handle)

    pool.close()    

    return 

def Multiprocess_T2D():

    InstanceOfDict = defaultdict(list)
    LevenshteinDict = defaultdict(list)

    List1 =  ['32051842_5_1757891723986774036.csv', '93702496_0_39013305210829591.csv', '54112392_0_160015365721559357.csv', '9475172_1_1023126399856690702.csv', '51130304_0_3035822254995425429.csv', '77632062_0_2292892375652659825.csv', '51741865_0_9203644246202164492.csv', '90593344_0_8311455501234425088.csv', '76275868_0_4528346580716713695.csv', '90723763_0_874013697986950454.csv', '14311244_0_7604843865524657408.csv', '52572391_0_8684896999787304275.csv', '44206774_0_3810538885942465703.csv', '67865873_0_8259587610208037357.csv','44005578_0_5400481538370055338.csv','98458255_0_5873264818593370400.csv'] #16
    List2 =  ['32460905_0_5195150472060854231.csv', '73242003_5_4847571983313033360.csv', '88523363_0_8180214313099580515.csv', '55027702_0_628532586316851176.csv', '98929678_0_3700213490979945526.csv', '72017594_0_7055198579167316152.csv', '70404164_0_5498195978476333642.csv', '79966524_0_1484492112455633784.csv', '12193237_0_8699643798888088574.csv', '29414811_2_4773219892816395776.csv', '38390205_1_8150763005563658139.csv', '48456557_0_3760853481322708783.csv', '86747932_0_7532457067740920052.csv','10630177_0_4831842476649004753.csv','45533669_1_7463037039059239824.csv','28079336_2_2139598421036200532.csv'] #16
    List3 =  ['29412826_0_3422707769191426540.csv', '19654359_0_3020273135042459469.csv', '74685423_0_8061289760697103949.csv', '37856682_0_6818907050314633217.csv', '30166286_0_1837439231599898748.csv', '9834884_0_3871985887467090123.csv', '86297395_0_6919201319699354263.csv', '3389822_6_374624044314151266.csv', '28993628_1_6072768612274729703.csv', '8468806_0_4382447409703007384.csv', '40844462_1_6230938203735169234.csv', '53989675_0_8697482470743954630.csv', '29886325_0_1448173912684571475.csv', '53583534_0_503521622577229691.csv','68442591_0_8506821153001360777.csv','57943722_0_8666078014685775876.csv'] #16
    List4 =  ['75154185_0_1607627349983289012.csv', '24036779_0_5608105867560183058.csv', '22987653_0_7774852112768859306.csv', '9686394_0_5980699488362107028.csv', '61753761_9_4735181228515289890.csv', '21245481_0_8730460088443117515.csv', '12183399_0_6725061928072492226.csv', '45593932_0_6869375743660252130.csv', '47709681_0_4437772923903322343.csv', '4444204_0_951426790527152756.csv', '98312357_0_6395417306876145729.csv', '77694908_0_6083291340991074532.csv', '68779923_5_2067037721454758189.csv','28036255_0_5705563063166785494.csv','34899692_0_6530393048033763438.csv','37541068_1_1156932518917623732.csv'] # 16
    List5 =  ['74491133_0_7177831100884797849.csv', '41819807_0_3139249984404131679.csv', '64499281_8_7181683886563136802.csv', '80588006_0_6965325215443683359.csv', '1614988_0_8789868670151796042.csv', '88732174_0_7321365014357502211.csv', '41648740_0_6959523681065295632.csv', '68779923_4_1832350971585698643.csv', '49087820_0_461228320225084868.csv', '36102169_0_7739454799295072814.csv', '29021592_3_2299138476894681059.csv', '54773498_0_4341462266113591159.csv', '24850757_0_5978004733768297283.csv', '40534006_0_4617468856744635526.csv','34041816_1_4749054164534706977.csv','41194422_1_1172256090552737974.csv'] # 16
    List6 =  ['91474256_0_964747655387573523.csv', '10151359_0_8168779773862259178.csv', '50270082_0_444360818941411589.csv', '33167985_0_5220049369716352813.csv', '48944826_0_2321751364268052533.csv', '3962429_0_3452614694863053249.csv', '68779923_2_1000046510804975562.csv', '39606331_0_1646213676302448706.csv', '5873256_0_7795190905731964989.csv', '26310680_0_5150772059999313798.csv', '41336118_0_4331895026409635103.csv', '71137051_0_8039724067857124984.csv', '8286121_0_8471791395229161598.csv', '15700884_0_8204220284166767221.csv','84675086_0_6386416780680710863.csv','39107734_2_2329160387535788734.csv'] #16
    List7 =  ['66329615_0_3585856495110277641.csv', '28423212_0_2742990779532526553.csv', '61069974_0_1894536874590565102.csv', '71840765_0_6664391841933033844.csv', '49772588_0_6549847739640234347.csv', '68779923_1_3240042497463101224.csv', '88353875_0_4876234304797064286.csv', '55961337_0_6548713781034932742.csv', '8009320_0_5507755752653817624.csv', '39173938_0_7916056990138658530.csv', '28079336_1_3124145965038277571.csv', '79327346_0_5787863739697336601.csv', '24859353_0_7027810986004269522.csv', '73988811_0_2775758476756716904.csv','74718406_0_3914457597478137388.csv','24417511_0_3058323105166963431.csv'] #16
    List8 =  ['11599512_1_280388135214354946.csv', '69881946_0_1105130426898457358.csv', '6869358_0_1379459120563510331.csv', '38732532_0_1420803645394505878.csv', '33401079_0_9127583903019856402.csv', '29021592_4_4348860179678308511.csv', '29414811_6_8221428333921653560.csv', '11688006_0_8123036130090004213.csv', '13719111_1_5719401842463579519.csv', '26270372_1_3774607938887520564.csv', '57938705_0_8737506792349461963.csv', '60319454_0_3938426910282115527.csv', '19272019_0_1508498249156534553.csv', '35188621_0_6058553107571275232.csv', '13395751_3_369722706637560735.csv','78005071_0_8643954006825971723.csv'] #16
    List9 =  ['63389809_0_8179819543692215824.csv', '64207896_0_3206705146602325658.csv', '39759273_0_1427898308030295194.csv', '70515524_0_5425995199745290007.csv', '4730513_0_8573199053866072336.csv', '41194422_0_7231546114369966811.csv', '61121469_0_6337620713408906340.csv', '21362676_0_6854186738074119688.csv', '52299421_0_4473286348258170200.csv', '14067031_0_559833072073397908.csv', '49801939_0_6964113429298874283.csv', '3887681_0_7938589465814037992.csv', '33242247_1_7232384061498591611.csv', '21329809_0_5526008408364682899.csv', '29414811_12_251152470253168163.csv'] #15
    List10 = ['12125836_0_1134348206297032434.csv', '38428277_0_1311643810102462607.csv', '69381058_0_984708047398578866.csv', '96203994_0_2127964719640427252.csv', '37815699_0_8391788584341609964.csv', '4501311_8_8306082458935575308.csv', '21333456_2_1886495893795687264.csv', '5972322_0_3150366936082777235.csv', '61626303_0_266372209664087642.csv', '12271141_0_8517913935669973086.csv', '28646774_0_3256889721737611537.csv', '55004961_0_2904467548072189860.csv', '16767252_0_2409448375013995751.csv', '90196673_0_5458330029110291950.csv', '62564020_0_3836030043284699244.csv'] #15
    List11 = ['89511064_0_2199624509082573904.csv', '69537082_0_7789694313271016902.csv', '14380604_4_3329235705746762392.csv', '21801207_0_8144839668470123042.csv', '2066887_0_746204117268168398.csv', '99070098_0_2074872741302696997.csv', '41480166_0_6681239260286218499.csv', '43226272_0_5995103035300388365.csv', '42764224_0_515744189130384109.csv', '97941125_0_8220652154649529701.csv', '29857217_0_961038783966485949.csv', '32812626_0_5428724146873158593.csv', '19361188_0_1640726405141876003.csv', '25933880_0_4058554985574416690.csv', '48805028_0_8933391169600128370.csv'] #15
    List12 = ['21585935_0_294037497010176843.csv', '66009064_0_9148652238372261251.csv', '45073662_0_3179937335063201739.csv', '28086084_0_3127660530989916727.csv', '53822652_0_5767892317858575530.csv', '64734047_0_5880705307495695536.csv', '21337553_0_8832378999628437599.csv', '63450419_0_8012592961815711786.csv', '92896255_0_4593968708343548527.csv', '18422942_0_3659164506677528063.csv', '46671561_0_6122315295162029872.csv', '93078729_0_1451251196788678526.csv', '55238374_0_3379409961751009152.csv', '80184932_0_4240003884724905487.csv', '41839679_0_3009344178474568371.csv'] #15
    List13 = ['11278409_0_3742771475298785475.csv', '52208141_2_5171684379987586712.csv', '28154036_0_909001972154294543.csv', '25404227_0_2240631045609013057.csv', '78650283_0_5937576517849452289.csv', '76373906_0_6904594838446307331.csv', '84548468_0_5955155464119382182.csv', '9567241_0_5666388268510912770.csv', '86627271_6_2239821927452848323.csv', '39650055_5_7135804139753401681.csv', '28494901_6_7026744149694237309.csv', '50245608_0_871275842592178099.csv', '70883081_0_5357790421435123524.csv', '78891639_0_3299957631631122948.csv', '10579449_0_1681126353774891032.csv'] #15
    List14 = ['64092785_0_4696367782987533337.csv', '75367212_2_2745466355267233390.csv', '16949304_0_2573050770332101882.csv', '12746760_0_6703465836620308483.csv', '43469354_0_6029017163991310319.csv', '29414811_13_8724394428539174350.csv', '30645756_0_2413317245475669227.csv', '3917335_0_7791699395300625164.csv', '11833461_1_3811022039809817402.csv', '12151277_0_1262954734066545952.csv', '76836443_2_249332777049648171.csv', '84575189_0_6365692015941409487.csv', '56224555_0_3713922722778385817.csv', '54719588_0_8417197176086756912.csv', '20135078_0_7570343137119682530.csv'] #15
    List15 = ['36039980_4_4283009829602711082.csv', '1146722_1_7558140036342906956.csv', '22864497_0_8632623712684511496.csv', '58891288_0_1117541047012405958.csv', '68779923_0_3859283110041832023.csv', '46646666_0_5802598112171303204.csv', '27466715_0_3913547177671701530.csv', '13357355_0_5384862281417742530.csv', '9206866_1_8114610355671172497.csv', '43237185_1_3636357855502246981.csv', '19073331_0_2742992342272078110.csv', '90325533_0_6668263142989366843.csv', '24142265_0_4577466141408796359.csv', '91959037_0_7907661684242014480.csv', '7929299_0_3253718003116029649.csv'] #15

    TableList = [List1, List2, List3, List4, List5, List6, List7, List8, List9, List10, List11, List12, List13, List14, List15]
    

    pool = multiprocessing.Pool()
    #input list, out is list of tuple
    results = pool.map(ReadTable, TableList) #--> hamechi tu X bud.
    
    x = [result[0] for result in results]
    print("X:",x)
    for i in x:
        if not(i == {}):
            LevenshteinDict = {**LevenshteinDict,**i}

    y = [result[1] for result in results]
    print("Y:",y)
    for i in y:
        if not(i == {}):
            InstanceOfDict = {**InstanceOfDict, **i}

    print("Final Levenshtein Dict:",LevenshteinDict)
    print("Final Types Dict",InstanceOfDict)
    
    #save dictionary of label:instanceof to pickle file
    with open('data/T2DTypes.pickle', 'wb') as handle:
        pickle.dump(InstanceOfDict, handle)
    #save dictionary of Levenshtein code to pickle file
    with open('data/T2DLevenshtein.pickle', 'wb') as handle:
        pickle.dump(LevenshteinDict, handle)

    pool.close()  

    return

if __name__ == '__main__':

    starttime = time.time()
    #Multiprocess_Limaye()
    Multiprocess_T2D()
    print('That took {} seconds'.format(time.time() - starttime))

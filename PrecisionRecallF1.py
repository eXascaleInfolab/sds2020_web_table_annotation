def Precision_Recall_F1(TP,FN,FP):

	#metrics:
	#todo : u must sum up all the TP of all the tables! not like this
	try:
		precision = TP / (TP+FP)
	except ZeroDivisionError:
		precision = "Undefined"
	try:
		recall = TP / (TP+FN)
	except: 
		recall = "Undefined"
	if(precision == "Undefined" or recall =="Undefined" or (precision+recall==0) ):
		F1 = "Undefined"
	else:
		F1 = 2*((precision*recall)/(precision+recall))

	print("Precision : ",precision)
	print("Recall : ",recall)
	print("F1 : ",F1)

	return(precision, recall, F1)



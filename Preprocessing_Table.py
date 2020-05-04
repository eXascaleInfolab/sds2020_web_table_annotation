#it is now used only for the embedding part
#to be merged with the same steps in Fullphase code as one preprocessing file.
import html
import string
import re
import pandas as pd
import LabelColumn as LC
import ReferenceColumn as RC
from dateutil.parser import parse

def get_col_dtype(col):
        """
        Infer datatype of a pandas column, process only if the column dtype is object. 
        input:   col: a pandas Series representing a df column. 
        """

        if col.dtype =="object":
            # try numeric
            try:
                col_new = pd.to_datetime(col.dropna().unique())
                return col_new.dtype
            except:
                try:
                    col_new = pd.to_numeric(col.dropna().unique())
                    return col_new.dtype
                except:
                    try:
                        col_new = pd.to_timedelta(col.dropna().unique())
                        return col_new.dtype
                    except:
                        return "object"

        else:
            return col.dtype

def Preprocessing_Table(T):
	#index of removed columns from table
	removed_col_index = []	

    #replace html characters reference in tables
	for col in T:
		#print("This is T[col] before replace html ref", T[col])
		try:
			T[col] = [html.unescape(elem) for elem in T[col]]
			#remove first spaces
			T[col] = [re.sub(r"^\s+", "", elem) for elem in T[col]]
			#remove last spaces
			T[col] = [re.sub(r"\s+$", "", elem) for elem in T[col]]

			#remove dates from string
			#because of try catch in one line for loop, i used exec
			try:
				exec("try:T[col] = [ ''.join(parse(elem, fuzzy_with_tokens=True)[1]) for elem in T[col]]\nexcept ValueError:print('dates handled!')")
			except OverflowError:
				try:
					T[col] = [ ''.join(parse(elem, fuzzy_with_tokens=True)[1]) for elem in T[col]]
					continue
				except Exception:
					continue
					
		except TypeError:
			print("Type not suitable for iteration")
			continue
		#print("This is T[col] after replace html ref", T[col])

	#remove punctuations and html characters reference from tables
	for col in T:
		try:
			exec("try:T[col] = [ ''.join([ch for ch in elem if ch not in string.punctuation]) for elem in T[col]]\nexcept ValueError:print('punctuation not removed!')")
		except Exception:
			print('punctuation cant be fixed!')
		
    #replace html characters reference in tables
	for col in T:
		try:
			#remove first spaces
			T[col] = [re.sub(r"^\s+", "", elem) for elem in T[col]]
			#remove last spaces
			T[col] = [re.sub(r"\s+$", "", elem) for elem in T[col]]
		except TypeError:
			print("Type not suitable for iteration")
			continue


	#removing numeric datatypes
	T = T.select_dtypes(exclude=['int_','float_','complex_'])

	for col in T:
		T.replace(r'[-+]?[0-9]*\.?[0-9]*%', '', regex=True, inplace=True)
		exec("try:T[col] = [re.sub('^[0-9 ]+$','',elem) for elem in T[col]]\nexcept TypeError:print('cannot replace numeric values!')")
		#remove if the pandas object is numeric! or date!
		print("This is dtype:",get_col_dtype(T[col]))
		if(get_col_dtype(T[col]) in ['int_','float_','datetime64[ns]']):
			#col indexe column hast ke bayad pak she!
			T = T.drop(col, axis=1)
			removed_col_index.append(col)
	
	#a hack to reset. the first T my dataframe. the second and last is not my table		
	T = T.T.reset_index(drop=True).T
	#1.T' <- T;	
	TPrime = T
	#----------------------------------Sample phase----------------------------------------

	#4.labelColumn  <- getLabelColumn(T);
	labelColumn = LC.getLabelColumn(T)
	print("-" * 50)
	print(" " * 50)
	print("-" * 50)
	#TODO: change this index keeping to a sustainable method
	#you can make annotations dictionary of dictionaries instead of dictionary of lists that they are right now
	for rci in removed_col_index:
		if(labelColumn > rci ):
			labelColumn = labelColumn - 1
			print("RCI:",rci)

	print("label column after drop index substraction:",labelColumn)
	#5.referenceColumns <- getReferenceColumns(T);
	referenceColumns = RC.getReferenceColumns(T, labelColumn)
	
    
	return(T,labelColumn,referenceColumns)
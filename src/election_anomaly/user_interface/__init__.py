#!usr/bin/python3

def pick_one(dataframe,item='row'):
	"""Returns index of item chosen by user"""
	# TODO check that index entries are ints (or handle non-int)
	print(dataframe)
	choice = int(input('Enter the {} of the desired {} (or hit return if none is correct):\n'.format(dataframe.index.name,item)))
	if choice == '':
		return None
	else:
		while choice not in dataframe.index:
			choice = input('Entry must be in {}. Please try again.'.format(dataframe.index))
		return choice
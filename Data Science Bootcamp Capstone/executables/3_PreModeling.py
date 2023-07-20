#!/usr/bin/env python
# coding: utf-8

# # <a id='toc1_'></a>[Pre Modeling](#toc0_)
# 
# """
# Created on Sun Jul  2 15:34:08 2023
# 
# @author: Nima
# """

# **Table of contents**<a id='toc0_'></a>    
# - [Pre Modeling](#toc1_)    
# - [1. Introduction](#toc2_)    
#   - [1.1. Key Questions](#toc2_1_)    
#   - [1.2. Assumptions and Methods](#toc2_2_)    
#   - [1.3. Setup and Data Collection](#toc2_3_)    
# - [2. Transformation](#toc3_)    
#   - [2.1 Dropping columns used for eda](#toc3_1_)    
#   - [2.2 Transforming Date](#toc3_2_)    
# - [3. Correlation](#toc4_)    
#   - [3.1. Correlation Matrix](#toc4_1_)    
#   - [3.2. Statistically Significance](#toc4_2_)    
#   - [3.3. Plotting Correlation](#toc4_3_)    
#   - [3.4 Correlation between independant variables](#toc4_4_)    
# - [4. Pre-Modeling](#toc5_)    
#   - [4.1. Implied Order](#toc5_1_)    
#   - [4.2. One-Hot Encoding ](#toc5_2_)    
#     - [4.2.1. Room Type ](#toc5_2_1_)    
#     - [4.2.2. Bathroom Type ](#toc5_2_2_)    
#     - [4.2.3. Neighbourhood Cleansed ](#toc5_2_3_)    
#     - [4.2.4 Property Type](#toc5_2_4_)    
# - [4. Multicollinearity](#toc6_)    
#   - [4.1 Eigen Vector](#toc6_1_)    
#   - [4.1 Variance Inflation Factor (VIF) ](#toc6_2_)    
# - [5. Save Transformations](#toc7_)    
# - [6. Key Findings](#toc8_)    
# 
# <!-- vscode-jupyter-toc-config
# 	numbering=false
# 	anchor=true
# 	flat=false
# 	minLevel=1
# 	maxLevel=6
# 	/vscode-jupyter-toc-config -->
# <!-- THIS CELL WILL BE REPLACED ON TOC UPDATE. DO NOT WRITE YOUR TEXT IN THIS CELL -->

# # <a id='toc2_'></a>[1. Introduction](#toc0_)
# 
# 

# ## <a id='toc2_1_'></a>[1.1. Key Questions](#toc0_)

# This file focuses on adapting the dataset for machine learning technics. 
# We will also be looking at the correlation between the variables

# ## <a id='toc2_2_'></a>[1.2. Assumptions and Methods](#toc0_)

# - Remaining missing value or unknown were replaced with their means.
# 
# - Based on the correlation between the independant variables and between the dependant and the independent variables, the data was narrowed. 
# When the correlation was higher than -0.7 or 0.7 between the independant variable, we considered it as a high correlation. We then compare the correlation between the dependant variable and the couple having multicollinearity. The member of the couple with the lowest correlation to log price was filtered from the dataset. 
# Finally a multicollinearity test has been done as well.
# 
# - Categorical variables with an order were transformed within the same column. Categorical variables without an explicit order, were encoded with the one-hot methodology. The last columns of transformation was removed to avoid multicolinearity.
# 

# 
# ## <a id='toc2_3_'></a>[1.3. Setup and Data Collection](#toc0_)
# 
# 

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import os
import nbformat
from nbconvert import PythonExporter
from statsmodels.stats.outliers_influence import variance_inflation_factor


# In[2]:


# Reading the configuration file
exec(open('executables/0_Config.py').read())


# In[3]:


# Reading previous files
csv_path = 'outputs/2_eda_airbnb_vancouver.csv'

if os.path.isfile(csv_path):
    df_subset = pd.read_csv(csv_path)
    df_subset['first_review'] = pd.to_datetime(df_subset['first_review']) # CSV does not saved it as a date
    df_subset['last_review'] = pd.to_datetime(df_subset['last_review'])
    df_subset['host_since'] = pd.to_datetime(df_subset['host_since'])
    print('CSV file read successfully.')
else:
    exec(open('executables/2_EDA.py').read())
    print('File execution completed.')


# 
# # <a id='toc3_'></a>[2. Transformation](#toc0_)

# 
# ## <a id='toc3_1_'></a>[2.1 Dropping columns used for eda](#toc0_)

# In[4]:


# delete 'amenities' and host_verification column since we created binary variable for them in feature engineering
df_subset.drop(['amenities', 'host_verifications'], axis=1, inplace=True)
# deleting columns created for EDA. this information exists in other columns
df_subset.drop(['bedroom_category','neighborhood_mapping','availability_grp'], axis=1, inplace=True)
# deleting column replace by price
df_subset.drop(['price'], axis=1, inplace=True)


# 
# ## <a id='toc3_2_'></a>[2.2 Transforming Date](#toc0_)

# In[5]:


# Extract the year, month, and day into separate columns
df_subset['fr_year'] = df_subset['first_review'].dt.year
df_subset['fr_month'] = df_subset['first_review'].dt.month
df_subset['fr_day'] = df_subset['first_review'].dt.day
    
# Extract the year, month, and day into separate columns
df_subset['lr_year'] = df_subset['last_review'].dt.year
df_subset['lr_month'] = df_subset['last_review'].dt.month
df_subset['lr_day'] = df_subset['last_review'].dt.day

# Extract the year, month, and day into separate columns
df_subset['hs_year'] = df_subset['host_since'].dt.year
df_subset['hs_month'] = df_subset['host_since'].dt.month
df_subset['hs_day'] = df_subset['host_since'].dt.day


# In[6]:


df_subset.drop(['host_since', 'last_review', 'first_review'], axis=1, inplace=True)


# 
# # <a id='toc4_'></a>[3. Correlation](#toc0_)

# 
# ## <a id='toc4_1_'></a>[3.1. Correlation Matrix](#toc0_)

# In[7]:


corr_matrix = df_subset.drop(df_subset.select_dtypes('object').columns, axis=1).corr()

# Calculate the correlation matrix
corr_matrix = df_subset.corr()

# Find the correlation values with Log Price
log_price_corr = corr_matrix.log_price

# Filter for positive and negative correlations
positive_corr = log_price_corr[log_price_corr > 0].sort_values(ascending=False).head(10)
negative_corr = log_price_corr[log_price_corr < 0].sort_values().head(3)

# Print the results
print("Columns positively correlated with 'Log Price':")
print(positive_corr)
print()
print("Columns negatively correlated with 'Log Price':")
print(negative_corr)


# From the correlation matrix, the price of a property depends positvely on: accomodates, bedrooms, beds, bathrooms, type of room, but also the presence of a dishwasher among others. Longitude has also a small (negative) impact.

# ## <a id='toc4_2_'></a>[3.2. Statistically Significance](#toc0_)

# In[8]:


# Create an empty DataFrame to store the results
df_log_price_corr = pd.DataFrame(columns=['Variable', 't-value', 'p-value'])

# Loop through the top elements with positive correlations
for element in positive_corr.index:
    tval, p = stats.pearsonr(df_subset[element], df_subset['log_price'])
    df_log_price_corr = df_log_price_corr.append({'Variable': element, 't-value': tval, 'p-value': p}, ignore_index=True)

df_log_price_corr


# p values are smaller than 0.05, so all the covariance with log price and the variables are significative
# 

# 
# ## <a id='toc4_3_'></a>[3.3. Plotting Correlation](#toc0_)

# In[9]:


# Mask for upper-triangular
mask = np.triu(corr_matrix)

plt.figure(figsize=(100,150))
sns.heatmap(corr_matrix.round(1), annot=True, vmax=1, vmin=-1, cmap='coolwarm', mask=mask, center=0)
plt.title("Correlation Matrix",size=15, weight='bold')

# Save the plot as a JPEG file
plt.savefig('images/correlation_matrix.jpeg', format='jpeg')

# Close the figure to prevent it from being displayed in the notebook
plt.close()


# 
# ## <a id='toc4_4_'></a>[3.4 Correlation between independant variables](#toc0_)

# In[10]:


# Calculate the correlation matrix
corr_matrix = df_subset.corr()

# Keep variable with corr greater than 0.7 or less than -0.7
multi_col = (corr_matrix.abs() > 0.7) & (corr_matrix != 1)

# Use the multi_col to filter the correlation matrix
high_corr_matrix = corr_matrix[multi_col]

# Create a dataframe from the filtered correlation matrix
high_corr_df = pd.DataFrame(high_corr_matrix.stack()).reset_index()
high_corr_df.columns = ['Variable 1', 'Variable 2', 'Correlation']

# Sort the values in descending order based on the 'Correlation' column
high_corr_df = high_corr_df.sort_values(by='Correlation', ascending=False)

# Remove every second line
high_corr_df = high_corr_df.iloc[::2]

# Add new columns for log_price and Variable 1 correlation
high_corr_df['log_price_and_variable1'] = high_corr_df.apply(lambda row: corr_matrix.loc['log_price', row['Variable 1']], axis=1)

# Add new columns for log_price and Variable 2 correlation
high_corr_df['log_price_and_variable2'] = high_corr_df.apply(lambda row: corr_matrix.loc['log_price', row['Variable 2']], axis=1)

# Compare absolute values and display the variable with the lowest absolute correlation
high_corr_df['var_with_lowest_logprice_corr'] = np.where(
    np.abs(high_corr_df['log_price_and_variable1']) > np.abs(high_corr_df['log_price_and_variable2']),
    high_corr_df['Variable 2'],
    high_corr_df['Variable 1']
)


# In[11]:


# Display the dataframe
high_corr_df


# In[12]:


# Get the distinct values from the columns_to_drop list
columns_to_drop = list(set(high_corr_df['var_with_lowest_logprice_corr'].tolist()))

# Drop the columns from df_subset
df_subset = df_subset.drop(columns=columns_to_drop)


# One of the hypothesis of a linear regression is to avoid collinearity.
# When two variables have a collinearity, we removed one of them based on the one having the lower correlation with the dependant variable.

# 
# # <a id='toc5_'></a>[4. Pre-Modeling](#toc0_)
# 
# In this section, we will perform some transformation to adapt to machine learning's requirement for categorical variables

# 
# ## <a id='toc5_1_'></a>[4.1. Implied Order](#toc0_)

# In[13]:


# Label categorical variables with an implied order:
df_subset.host_response_time.unique()    
category_order = ['unknown', 'within an hour', 'within a few hours', 'within a day', 'a few days or more']

# Create a dictionary with category_order as keys and their corresponding indices as values
category_order = {category_order[i]: i for i in range(len(category_order))}

# Replace the keys in df_subset['host_response_time'] column with the corresponding values
df_subset['host_response_time_order'] = df_subset['host_response_time'].map(category_order)

# Reference table for meaning of host_response_time_order    
df_host_response=df_subset[['host_response_time_order','host_response_time']].drop_duplicates()

#Drop original column column
df_subset = df_subset.drop('host_response_time', axis=1)



# 
# ## <a id='toc5_2_'></a>[4.2. One-Hot Encoding ](#toc0_)

# Label Encoding collapses the categorical variable into a single numerical column, potentially leading to information loss.
# 
# One-Hot Encoding preserves all the information by creating separate binary columns,  but it can increase the dimensionality of the dataset. 
# 
# The trade-off between information loss > dimensionality.
# 
# Therefore I did a One-Hot for encoding the columns when there was no order in the catgorical column

# 
# ### <a id='toc5_2_1_'></a>[4.2.1. Room Type ](#toc0_)

# In[14]:


# Create dummy variables from room_type
dummy_df = pd.get_dummies(df_subset.room_type, prefix='room_type')
# Lowercase the values, Replace '/' with '_or_', Replace spaces with '_'
dummy_df.columns = dummy_df.columns.str.lower().str.replace('/', '_or_').str.replace(' ', '_')
# Convert True/False values to 1/0
dummy_df = dummy_df.astype(int)
# Concatenate the dummy variables with the original DataFrame
df_subset = pd.concat([df_subset, dummy_df], axis=1)
#Drop original column
df_subset = df_subset.drop('room_type', axis=1)
# Drop one of the encoding column
df_subset = df_subset.iloc[:, :-1]


# 
# ### <a id='toc5_2_2_'></a>[4.2.2. Bathroom Type ](#toc0_)

# In[15]:


# Create dummy variables from bathroom_type
dummy_df = pd.get_dummies(df_subset.bathroom_type, prefix='bathroom_is')
# Convert True/False values to 1/0
dummy_df = dummy_df.astype(int)
# Concatenate the dummy variables with the original DataFrame
df_subset = pd.concat([df_subset, dummy_df], axis=1)
#Drop original column
df_subset = df_subset.drop('bathroom_type', axis=1)
# Drop one of the encoding column
df_subset = df_subset.iloc[:, :-1]


# 
# ### <a id='toc5_2_3_'></a>[4.2.3. Neighbourhood Cleansed ](#toc0_)

# In[16]:


#Creating dummies by neighbourdhood_cleansed
dummy_df = pd.get_dummies(df_subset.neighbourhood_cleansed, prefix='ngb')
# Lowercase the values, Replace '-' with '_', Replace spaces with '_'
dummy_df.columns = dummy_df.columns.str.lower().str.replace('-', '_').str.replace(' ', '_')
# Convert True/False values to 1/0
dummy_df = dummy_df.astype(int)
# Concatenate the dummy variables with the original DataFrame
df_subset = pd.concat([df_subset, dummy_df], axis=1)
#Drop column
df_subset = df_subset.drop('neighbourhood_cleansed', axis=1)
# Drop one of the encoding column
df_subset = df_subset.iloc[:, :-1]


# 
# ### <a id='toc5_2_4_'></a>[4.2.4 Property Type](#toc0_)

# In[17]:


#Creating dummies by neighbourdhood_cleansed
dummy_df = pd.get_dummies(df_subset.property_type)
# Lowercase the values, Replace '-' with '_', Replace spaces with '_'
dummy_df.columns = dummy_df.columns.str.lower().str.replace('-', '_').str.replace(' ', '_')
# Convert True/False values to 1/0
dummy_df = dummy_df.astype(int)
# Concatenate the dummy variables with the original DataFrame
df_subset = pd.concat([df_subset, dummy_df], axis=1)
#Drop column
df_subset = df_subset.drop('property_type', axis=1)
# Drop one of the encoding column
df_subset = df_subset.iloc[:, :-1]


# 
# # <a id='toc6_'></a>[4. Multicollinearity](#toc0_)

# 
# ## <a id='toc6_1_'></a>[4.1 Eigen Vector](#toc0_)

# In[18]:


# Calculate the eigenvalues and eigenvectors of the correlation matrix
multicollinearity, V = np.linalg.eig(corr_matrix)

# Set the threshold for multicollinearity (e.g., 0.1)
multicollinearity_threshold = 0.1

# Filter eigenvalues based on the threshold
multicollinear_eigenvalues = [eig for eig in multicollinearity if eig > multicollinearity_threshold]

# Round eigenvalues to 4 decimal places
multicollinear_eigenvalues = np.round(multicollinear_eigenvalues, 4)

# Convert eigenvalues to regular decimal format (no exponential notation)
multicollinear_eigenvalues = ["{:.4f}".format(eig) for eig in multicollinear_eigenvalues]

# Print the updated eigenvalues
print("Eigenvalues with multicollinearity:")
print(multicollinear_eigenvalues)


# From the Eigenvalues, we have multiple cases of multicollinearity (when values are bigger than 0.01). We will investigate the multicollinearity with a different method to gather more insights

# 
# ## <a id='toc6_2_'></a>[4.2 Variance Inflation Factor (VIF) ](#toc0_)

# In[19]:


# Create an empty DataFrame to store the VIF results
vif_df = pd.DataFrame()

# Iterate over each column in the dataset
for idx, column in enumerate(df_subset.columns):
    try:
        # Calculate the VIF for the current variable
        vif = variance_inflation_factor(df_subset.values, idx)
        # Append the results to the DataFrame
        vif_df = vif_df.append({'Variable': column, 'VIF': vif}, ignore_index=True)
    except TypeError:
        print(f"Error calculating VIF for column: {column}")


# In[20]:


# Set the float format to avoid exponential notation
pd.options.display.float_format = '{:.2f}'.format

vif_df[vif_df['VIF']>5]


# As we can see, there are some multicollinearity issues. We can expect our linear modeling prediction to be weak because of it. However multicollinearity is tolerated - to a certain extend - for the more advances modeling we will be using. Therefore, we will not narrowing more the data. Instead we will regularization methods like Ridge Regression for the linear regression

# 
# # <a id='toc7_'></a>[5. Save Transformations](#toc0_)

# Save CSV

# In[21]:


df_subset.to_csv('outputs/3_pm_airbnb_vancouver.csv', index=False)


# Save Python Script

# In[22]:


if __name__ == '__main__':
    notebook_file_path = '3_PreModeling.ipynb'
    python_file_path = 'executables/3_PreModeling.py'

    convert_notebook_to_python(notebook_file_path, python_file_path)


# 
# # <a id='toc8_'></a>[6. Key Findings](#toc0_)

# There is some multicollinearity between our dependent variables, which will affect the linear modeling prediction. It's therefore required to use more advances modeling, which cope better with this problem.

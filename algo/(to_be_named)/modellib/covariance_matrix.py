import numpy as np
import pandas as pd

# beta_matrix is the beta covariance
# beta_expected_matrix is the expected value of beta
# pca_cov is the pca covariance matrix
# pca_array is the expected value of pca
# s_rt is the array of stock return
# error_cov is the var-cov matrix of errors, dimension = 2
# (changed to 1d array containing all variances, dimension=1)


def covariance_matrix(s_rt, beta_matrix, beta_expected_matrix, pca_cov, pca_array, error_cov):
    l = len(s_rt)
    whole_cov_matrix = pd.DataFrame(np.zeros((l,l)))
    for m in range(len(s_rt)):  # s_rt = stock_return and stock m 
        for n in range(len(s_rt)):  # stock n
            whole_cov = 0
            if m != n :  
                for x in range(len(pca_array)+1):  # x is the xth beta of stock m
                    for y in range(len(pca_array)+1):  # y is the yth beta of stock n
                        if x != 0 and y != 0:
                            single_cov = beta_expected_matrix[m,x]*beta_expected_matrix[n,y]*pca_cov[x-1,y-1]
                            whole_cov += single_cov
  
            # when m != n, 
            # correlation between error terms and correlation between error term and beta or PCi would be 0
                    
            if m == n:
                for x in range(len(pca_array)+1):
                    for y in range(len(pca_array)+1):                
                        if x == 0 and y == 0:
                            single_cov = beta_matrix[m,x,y] 
                            whole_cov = whole_cov + single_cov
                        if x != 0 and y == 0:
                            single_cov = pca_array[x-1] * beta_matrix[m,x,y]
                            whole_cov = whole_cov + single_cov * 2
                        if x != 0 and y != 0:
                            single_cov = beta_expected_matrix[m,x]*beta_expected_matrix[m,y]*pca_cov[x-1,y-1] + pca_array[x-1]*pca_array[y-1]*beta_matrix[m,x,y] + pca_cov[x-1,y-1]*beta_matrix[m,x,y] 
                            whole_cov += single_cov
                whole_cov = whole_cov + error_cov[m]
            whole_cov_matrix.loc[m,n] = whole_cov
            
    return(whole_cov_matrix)
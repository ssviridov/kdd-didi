from scipy.optimize import linear_sum_assignment
from scipy.sparse import csr_matrix
from itertools import count

def match(dispatch_observ, order_id='order_id', driver_id='driver_id', 
    weight="reward_units", maximize=True):

    driver_counter = count()
    order_counter = count()
    driver_dict = dict()
    order_dict = dict()
    result_dict = dict()
    weights = []
    driver_inds = []
    order_inds = []

    for item in dispatch_observ:
        weights.append(item[weight])

        driver = item[driver_id]
        driver_ind = driver_dict.get(driver, next(driver_counter))
        driver_dict[driver] = driver_ind
        driver_inds.append(driver_ind)   
        
        order = item[order_id]
        order_ind = order_dict.get(order, next(order_counter))
        order_dict[order] = order_ind
        order_inds.append(order_ind) 

        result_dict[(driver_ind, order_ind)] = dict(driver_id=driver, order_id=order)

    cost = csr_matrix((weights, (driver_inds, order_inds)), shape=(next(driver_counter), next(order_counter))).todense()

    row_ind, col_ind = linear_sum_assignment(cost, maximize)

    result = [result_dict[key] for key in zip(row_ind, col_ind) if key in result_dict]

    return cost[row_ind, col_ind].sum(), result


    


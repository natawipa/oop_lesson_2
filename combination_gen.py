import copy


def gen_comb_list(list_set):
    '''
        Parameters:
            list_set: a list of lists where each contains at least one element

        Returns:
            a list of lists, each of which is made from a combination of elements in each list in list_set

        Examples:
            gen_comb_list([[1, 2, 3]]) returns [[1], [2], [3]]
            gen_comb_list([[1, 2, 3], [4, 5]]) returns [[1, 4], [2, 4], [3, 4], [1, 5], [2, 5], [3, 5]]
            gen_comb_list([[1, 2, 3], [4, 5], [6, 7, 8]]) returns [[1, 4, 6], [2, 4, 6], [3, 4, 6], [1, 5, 6], [2, 5, 6], [3, 5, 6], [1, 4, 7], [2, 4, 7], [3, 4, 7], [1, 5, 7], [2, 5, 7], [3, 5, 7], [1, 4, 8], [2, 4, 8], [3, 4, 8], [1, 5, 8], [2, 5, 8], [3, 5, 8]]
    '''
    # if not list_set:
    #     return [[]]
    #
    # result = [[]]
    # for lst in list_set:
    #     result = [x + [y] for x in result for y in lst]
    #
    # return result

    if len(list_set) == 1:
        result =  []
        for item in list_set[0]:
            result.append([item])
        return result
    comb_list = gen_comb_list(list_set[0:-1])
    result = []
    for ls_item in comb_list:
        for val in list_set[-1]:
            _item = copy.deepcopy(ls_item)
            _item.append(val)
            result.append(_item)
    return result

gen_comb_list([[1, 2, 3]])
gen_comb_list([[1, 2, 3], [4, 5]])
gen_comb_list([[1, 2, 3], [4, 5], [6, 7, 8]])


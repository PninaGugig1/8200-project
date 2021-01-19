def rotate(nums, k):
    x = nums[k:] + nums[0:k]
    while nums: nums.pop()
    for i in x:
        nums.append(i)

nums = [1, 2, 3, 4, 5, 6, 7]
rotate(nums, 3)
print(nums)
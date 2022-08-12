class Solution(object):
    def lengthOfLongestSubstring(self, s):
        """
        :type s: str
        :rtype: int
        """
        a = {}
        count = 0
        max = 0
        for x in s:
            if x in a:
                a[x] += 1
            else:
                a[x] = 1
            if a[x] == 1:
                count += 1
            else:
                if count > max:
                    max = count
                index = list(a).index(x)
                while index >= 0:
                    a.pop(list(a)[index])
                    index -= 1
                count = list(a.values()).count(1) + 1
                a[x] = 1
        if count > max:
            max = count
        return max
print(Solution().lengthOfLongestSubstring("ohomm"))
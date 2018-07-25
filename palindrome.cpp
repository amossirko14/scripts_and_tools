bool isPalindrome(string s)
{
	s.erase(remove_if(s.begin(), s.end(), [](char ch){ return !isalnum(ch); }), s.end());
	transform(s.begin(), s.end(), s.begin(), ::toupper);
	return equal(s.begin(), s.begin() + s.size() / 2, s.rbegin());
}

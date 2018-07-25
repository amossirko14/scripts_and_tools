#include <iostream>
#include <string>
#include <algorithm>
#include <iomanip>
#include <vector>

using namespace std;

bool hexStrToDecimal(string hex_str, int * decimal);

int main()
{
	int decimal = 0;
	
	vector<string> vec_str = 
	{
		"0x20", "0x1234", "0X1234ab", "0x12345678H", "x1234H", "1234H",
		"12345678", "87654321", "f1234567", "7fffffff", "123456789", "0xffffffffH",
		"", "0XH", "0x1h", "0x0h", "0x0001", "0xabcg120", "0xabc.def",
	};

	for(auto & str : vec_str)
	{
		cout << setw(15) << str << setw(10) << boolalpha << hexStrToDecimal(str, &decimal);
		cout << setw(15) << decimal << endl;
	}

	return 0;
}

bool hexStrToDecimal(string hex_str, int * decimal)
{
	if(decimal == NULL) return false;
	*decimal = 0;

	transform(hex_str.begin(), hex_str.end(), hex_str.begin(), ::toupper);
	if(hex_str.size() >= 2 && (hex_str.substr(0, 2) == "0X"))
	{
		hex_str.erase(hex_str.begin(), hex_str.begin() + 2);
	}

	if(!hex_str.empty() && hex_str.back() == 'H') hex_str.pop_back();

	for(auto & c : hex_str)
	{
		if(!(isdigit(c) || (c >= 'A' && c <= 'F'))) return false;
	}

	if(hex_str.empty() || hex_str.size() > 8) return false;//empty string or overflow
	//if(hex_str.size() == 8 && (isalpha(hex_str.front()) || hex_str.front() >= '8' )) return false;//overflow 

	int tmp_digit = 0;
	for(auto & c : hex_str)
	{
		tmp_digit = isalpha(c) ? c - 'A' + 10 : c - '0';
		*decimal = 16 * (*decimal) + tmp_digit;
	}

	return true;
}

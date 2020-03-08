
#include <iostream>
#include <csetjmp>

using namespace std;

class Rainbow{
public:
	Rainbow(){
		cout << "Rainbow()" << endl;
	}
	~Rainbow(){
		cout << "~Rainbow()" << endl;
	}
};

int main(){
retry:
	Rainbow rb;
	goto retry;
	system("pause");
}
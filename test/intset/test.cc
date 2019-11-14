#include <iostream>

class A {
public:
    virtual void Print() = 0;
};

template <typename T>
class B : public A {
public:
    T t;
    void Print() {
        std::cout << t << std::endl;
    }
};

int main() {
    B<int> b;
    b.Print();
    return 0;
}
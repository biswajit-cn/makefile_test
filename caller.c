#include <stdio.h>
#include <stdlib.h>
#include "libreconstructor.h"
#include <string.h>
#include <time.h>
int main() {
    clock_t t;
    char* inp_string = "SELECT article_id from article WHERE article_content LIKE '%Asperiores%'\0";
    t = clock();
    char* res= WrapperFunc(inp_string);
    printf("%s\n",res);
    t = clock() - t;
    double time_taken = ((double)t)/CLOCKS_PER_SEC;
    printf("WrapperFunc() took %f seconds to execute \n", time_taken);
    // printf("%s\n",res);
}
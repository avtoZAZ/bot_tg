#include <iostream>
#include <vector>
#include <limits>

using namespace std;

struct Result {
    int below_sum;
    int max_element;
    int max_row;
    int max_col;
    int on_above_sum;
    int min_element;
    int min_row;
    int min_col;
};

Result processMatrix(const vector<vector<int>>& matrix) {
    Result result;
    int n = matrix.size();
    int m = matrix[0].size();
    
    // Ініціалізація
    result.below_sum = 0;
    result.max_element = INT_MIN;
    result.max_row = -1;
    result.max_col = -1;
    
    result.on_above_sum = 0;
    result.min_element = INT_MAX;
    result.min_row = -1;
    result.min_col = -1;
    
    bool has_below = false;
    bool has_on_above = false;
    
    // Обхід матриці
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
            // Побічна діагональ: i + j = n - 1
            // Під діагоналлю: i + j > n - 1
            // На та над діагоналлю: i + j <= n - 1
            
            if (i + j > n - 1) {  // під побічною діагоналлю
                result.below_sum += matrix[i][j];
                has_below = true;
                
                if (matrix[i][j] > result.max_element) {
                    result.max_element = matrix[i][j];
                    result.max_row = i;
                    result.max_col = j;
                }
            }
            else {  // на та над побічною діагоналлю
                result.on_above_sum += matrix[i][j];
                has_on_above = true;
                
                if (matrix[i][j] < result.min_element) {
                    result.min_element = matrix[i][j];
                    result.min_row = i;
                    result.min_col = j;
                }
            }
        }
    }
    
    if (!has_below) {
        result.max_element = 0;
    }
    
    if (!has_on_above) {
        result.min_element = 0;
    }
    
    return result;
}

void printMatrix(const vector<vector<int>>& matrix) {
    cout << "Матриця:" << endl;
    for (const auto& row : matrix) {
        for (int elem : row) {
            cout << elem << "\t";
        }
        cout << endl;
    }
    cout << endl;
}

int main() {
    // Приклад матриці
    vector<vector<int>> matrix = {
        {5, 7, 3, 2},
        {8, 6, 4, 9},
        {1, 2, 3, 7},
        {4, 5, 6, 8}
    };
    
    printMatrix(matrix);
    
    Result result = processMatrix(matrix);
    
    cout << "=== РЕЗУЛЬТАТИ ===" << endl << endl;
    
    cout << "Частина ПІД побічною діагоналлю:" << endl;
    cout << "Сума: " << result.below_sum << endl;
    if (result.max_row != -1) {
        cout << "Максимальний елемент: " << result.max_element 
             << " на позиції [" << result.max_row << "][" << result.max_col << "]" << endl;
    }
    cout << endl;
    
    cout << "Частина НА та НАД побічною діагоналлю:" << endl;
    cout << "Сума: " << result.on_above_sum << endl;
    if (result.min_row != -1) {
        cout << "Мінімальний елемент: " << result.min_element 
             << " на позиції [" << result.min_row << "][" << result.min_col << "]" << endl;
    }
    
    return 0;
}
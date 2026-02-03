// osgeo_band_calc.c
#include "osgeo_utils.h"
#include "osgeo_band_calc.h"
#include <string.h>
#include <math.h>
#include <ctype.h>

// ==================== 波段计算器结构 ====================

typedef struct {
    GDALDatasetH hDS;
    int width;
    int height;
    int bandCount;
    double** bandCache;
    int* bandCacheLoaded;
} BandCalculatorContext;

// ==================== 表达式Token ====================

typedef enum {
    TOKEN_NUMBER,
    TOKEN_BAND,
    TOKEN_OPERATOR,
    TOKEN_FUNCTION,
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_COMMA,
    TOKEN_COMPARE,
    TOKEN_LOGIC,
    TOKEN_END,
    TOKEN_ERROR
} TokenType;

typedef struct {
    TokenType type;
    double numValue;
    int bandIndex;
    char op;
    char funcName[32];
    char compareOp[3];
    char logicOp[3];
} Token;

// ==================== 编译后的指令 ====================

typedef enum {
    OP_LOAD_CONST,
    OP_LOAD_BAND,
    OP_ADD,
    OP_SUB,
    OP_MUL,
    OP_DIV,
    OP_POW,
    OP_NEG,
    OP_FUNC_SQRT,
    OP_FUNC_ABS,
    OP_FUNC_SIN,
    OP_FUNC_COS,
    OP_FUNC_TAN,
    OP_FUNC_LOG,
    OP_FUNC_LOG10,
    OP_FUNC_EXP,
    OP_FUNC_FLOOR,
    OP_FUNC_CEIL,
    OP_FUNC_ROUND,
    OP_FUNC_MIN,
    OP_FUNC_MAX,
    OP_FUNC_POW,
    OP_CMP_GT,
    OP_CMP_GE,
    OP_CMP_LT,
    OP_CMP_LE,
    OP_CMP_EQ,
    OP_CMP_NE,
    OP_LOGIC_AND,
    OP_LOGIC_OR,
    OP_CONDITIONAL
} OpCode;

typedef struct {
    OpCode op;
    double constValue;
    int bandIndex;
} Instruction;

// ★★★ 关键修改：使用命名结构体 ★★★
struct CompiledExpression {
    Instruction* instructions;
    int count;
    int capacity;
    int* usedBands;
    int usedBandCount;
};

// ==================== 词法分析器 ====================

typedef struct {
    const char* expr;
    int pos;
    int length;
} Lexer;

static void lexerInit(Lexer* lex, const char* expr) {
    lex->expr = expr;
    lex->pos = 0;
    lex->length = (int)strlen(expr);
}

static void skipWhitespace(Lexer* lex) {
    while (lex->pos < lex->length && isspace(lex->expr[lex->pos])) {
        lex->pos++;
    }
}

static Token nextToken(Lexer* lex) {
    Token token;
    memset(&token, 0, sizeof(Token));

    skipWhitespace(lex);

    if (lex->pos >= lex->length) {
        token.type = TOKEN_END;
        return token;
    }

    char c = lex->expr[lex->pos];

    // 数字
    if (isdigit(c) || (c == '.' && lex->pos + 1 < lex->length && isdigit(lex->expr[lex->pos + 1]))) {
        token.type = TOKEN_NUMBER;
        char numBuf[64];
        int numLen = 0;
        while (lex->pos < lex->length &&
               (isdigit(lex->expr[lex->pos]) || lex->expr[lex->pos] == '.' ||
                lex->expr[lex->pos] == 'e' || lex->expr[lex->pos] == 'E' ||
                ((lex->expr[lex->pos] == '+' || lex->expr[lex->pos] == '-') &&
                 numLen > 0 && (numBuf[numLen-1] == 'e' || numBuf[numLen-1] == 'E')))) {
            if (numLen < 63) {
                numBuf[numLen++] = lex->expr[lex->pos];
            }
            lex->pos++;
        }
        numBuf[numLen] = '\0';
        token.numValue = atof(numBuf);
        return token;
    }

    // 波段引用 b1, B1, band1, BAND1
    if ((c == 'b' || c == 'B') && lex->pos + 1 < lex->length) {
        int startPos = lex->pos;
        lex->pos++;

        // 跳过可选的 "and"
        if (lex->pos + 3 <= lex->length &&
            (strncasecmp(&lex->expr[lex->pos], "and", 3) == 0)) {
            lex->pos += 3;
        }

        // 读取数字
        if (lex->pos < lex->length && isdigit(lex->expr[lex->pos])) {
            int bandIdx = 0;
            while (lex->pos < lex->length && isdigit(lex->expr[lex->pos])) {
                bandIdx = bandIdx * 10 + (lex->expr[lex->pos] - '0');
                lex->pos++;
            }
            token.type = TOKEN_BAND;
            token.bandIndex = bandIdx;
            return token;
        }

        // 不是波段引用，回退
        lex->pos = startPos;
    }

    // 比较运算符
    if (c == '>' || c == '<' || c == '=' || c == '!') {
        if (lex->pos + 1 < lex->length) {
            char next = lex->expr[lex->pos + 1];
            if ((c == '>' && next == '=') || (c == '<' && next == '=') ||
                (c == '=' && next == '=') || (c == '!' && next == '=')) {
                token.type = TOKEN_COMPARE;
                token.compareOp[0] = c;
                token.compareOp[1] = next;
                token.compareOp[2] = '\0';
                lex->pos += 2;
                return token;
            }
        }
        if (c == '>' || c == '<') {
            token.type = TOKEN_COMPARE;
            token.compareOp[0] = c;
            token.compareOp[1] = '\0';
            lex->pos++;
            return token;
        }
    }

    // 逻辑运算符
    if (c == '&' && lex->pos + 1 < lex->length && lex->expr[lex->pos + 1] == '&') {
        token.type = TOKEN_LOGIC;
        token.logicOp[0] = '&';
        token.logicOp[1] = '&';
        token.logicOp[2] = '\0';
        lex->pos += 2;
        return token;
    }
    if (c == '|' && lex->pos + 1 < lex->length && lex->expr[lex->pos + 1] == '|') {
        token.type = TOKEN_LOGIC;
        token.logicOp[0] = '|';
        token.logicOp[1] = '|';
        token.logicOp[2] = '\0';
        lex->pos += 2;
        return token;
    }

    // 函数名或标识符
    if (isalpha(c) || c == '_') {
        token.type = TOKEN_FUNCTION;
        int nameLen = 0;
        while (lex->pos < lex->length &&
               (isalnum(lex->expr[lex->pos]) || lex->expr[lex->pos] == '_')) {
            if (nameLen < 31) {
                token.funcName[nameLen++] = tolower(lex->expr[lex->pos]);
            }
            lex->pos++;
        }
        token.funcName[nameLen] = '\0';
        return token;
    }

    // 运算符和括号
    switch (c) {
        case '+':
        case '-':
        case '*':
        case '/':
        case '^':
            token.type = TOKEN_OPERATOR;
            token.op = c;
            lex->pos++;
            return token;
        case '(':
            token.type = TOKEN_LPAREN;
            lex->pos++;
            return token;
        case ')':
            token.type = TOKEN_RPAREN;
            lex->pos++;
            return token;
        case ',':
            token.type = TOKEN_COMMA;
            lex->pos++;
            return token;
    }

    token.type = TOKEN_ERROR;
    return token;
}

// ==================== 表达式编译器 ====================

static void initCompiledExpr(CompiledExpression* ce) {
    ce->capacity = 64;
    ce->count = 0;
    ce->instructions = (Instruction*)CPLMalloc(sizeof(Instruction) * ce->capacity);
    ce->usedBands = (int*)CPLMalloc(sizeof(int) * 32);
    ce->usedBandCount = 0;
}

static void freeCompiledExprInternal(CompiledExpression* ce) {
    if (ce->instructions) CPLFree(ce->instructions);
    if (ce->usedBands) CPLFree(ce->usedBands);
    ce->instructions = NULL;
    ce->usedBands = NULL;
}

static void emitInstruction(CompiledExpression* ce, OpCode op, double constVal, int bandIdx) {
    if (ce->count >= ce->capacity) {
        ce->capacity *= 2;
        ce->instructions = (Instruction*)CPLRealloc(ce->instructions,
                                                     sizeof(Instruction) * ce->capacity);
    }
    ce->instructions[ce->count].op = op;
    ce->instructions[ce->count].constValue = constVal;
    ce->instructions[ce->count].bandIndex = bandIdx;
    ce->count++;
}

static void addUsedBand(CompiledExpression* ce, int bandIdx) {
    for (int i = 0; i < ce->usedBandCount; i++) {
        if (ce->usedBands[i] == bandIdx) return;
    }
    if (ce->usedBandCount < 32) {
        ce->usedBands[ce->usedBandCount++] = bandIdx;
    }
}

// 前向声明
static int parseExpression(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parseLogicOr(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parseLogicAnd(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parseComparison(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parseAddSub(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parseMulDiv(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parsePower(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parseUnary(Lexer* lex, CompiledExpression* ce, Token* currentToken);
static int parsePrimary(Lexer* lex, CompiledExpression* ce, Token* currentToken);

static int parseExpression(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    return parseLogicOr(lex, ce, currentToken);
}

static int parseLogicOr(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (!parseLogicAnd(lex, ce, currentToken)) return 0;

    while (currentToken->type == TOKEN_LOGIC && strcmp(currentToken->logicOp, "||") == 0) {
        *currentToken = nextToken(lex);
        if (!parseLogicAnd(lex, ce, currentToken)) return 0;
        emitInstruction(ce, OP_LOGIC_OR, 0, 0);
    }
    return 1;
}

static int parseLogicAnd(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (!parseComparison(lex, ce, currentToken)) return 0;

    while (currentToken->type == TOKEN_LOGIC && strcmp(currentToken->logicOp, "&&") == 0) {
        *currentToken = nextToken(lex);
        if (!parseComparison(lex, ce, currentToken)) return 0;
        emitInstruction(ce, OP_LOGIC_AND, 0, 0);
    }
    return 1;
}

static int parseComparison(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (!parseAddSub(lex, ce, currentToken)) return 0;

    while (currentToken->type == TOKEN_COMPARE) {
        char op[3];
        strcpy(op, currentToken->compareOp);
        *currentToken = nextToken(lex);
        if (!parseAddSub(lex, ce, currentToken)) return 0;

        if (strcmp(op, ">") == 0) emitInstruction(ce, OP_CMP_GT, 0, 0);
        else if (strcmp(op, ">=") == 0) emitInstruction(ce, OP_CMP_GE, 0, 0);
        else if (strcmp(op, "<") == 0) emitInstruction(ce, OP_CMP_LT, 0, 0);
        else if (strcmp(op, "<=") == 0) emitInstruction(ce, OP_CMP_LE, 0, 0);
        else if (strcmp(op, "==") == 0) emitInstruction(ce, OP_CMP_EQ, 0, 0);
        else if (strcmp(op, "!=") == 0) emitInstruction(ce, OP_CMP_NE, 0, 0);
    }
    return 1;
}

static int parseAddSub(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (!parseMulDiv(lex, ce, currentToken)) return 0;

    while (currentToken->type == TOKEN_OPERATOR &&
           (currentToken->op == '+' || currentToken->op == '-')) {
        char op = currentToken->op;
        *currentToken = nextToken(lex);
        if (!parseMulDiv(lex, ce, currentToken)) return 0;
        emitInstruction(ce, op == '+' ? OP_ADD : OP_SUB, 0, 0);
    }
    return 1;
}

static int parseMulDiv(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (!parsePower(lex, ce, currentToken)) return 0;

    while (currentToken->type == TOKEN_OPERATOR &&
           (currentToken->op == '*' || currentToken->op == '/')) {
        char op = currentToken->op;
        *currentToken = nextToken(lex);
        if (!parsePower(lex, ce, currentToken)) return 0;
        emitInstruction(ce, op == '*' ? OP_MUL : OP_DIV, 0, 0);
    }
    return 1;
}

static int parsePower(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (!parseUnary(lex, ce, currentToken)) return 0;

    if (currentToken->type == TOKEN_OPERATOR && currentToken->op == '^') {
        *currentToken = nextToken(lex);
        if (!parsePower(lex, ce, currentToken)) return 0;
        emitInstruction(ce, OP_POW, 0, 0);
    }
    return 1;
}

static int parseUnary(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (currentToken->type == TOKEN_OPERATOR && currentToken->op == '-') {
        *currentToken = nextToken(lex);
        if (!parseUnary(lex, ce, currentToken)) return 0;
        emitInstruction(ce, OP_NEG, 0, 0);
        return 1;
    }
    if (currentToken->type == TOKEN_OPERATOR && currentToken->op == '+') {
        *currentToken = nextToken(lex);
        return parseUnary(lex, ce, currentToken);
    }
    return parsePrimary(lex, ce, currentToken);
}

static int parsePrimary(Lexer* lex, CompiledExpression* ce, Token* currentToken) {
    if (currentToken->type == TOKEN_NUMBER) {
        emitInstruction(ce, OP_LOAD_CONST, currentToken->numValue, 0);
        *currentToken = nextToken(lex);
        return 1;
    }

    if (currentToken->type == TOKEN_BAND) {
        int bandIdx = currentToken->bandIndex;
        addUsedBand(ce, bandIdx);
        emitInstruction(ce, OP_LOAD_BAND, 0, bandIdx);
        *currentToken = nextToken(lex);
        return 1;
    }

    if (currentToken->type == TOKEN_FUNCTION) {
        char funcName[32];
        strcpy(funcName, currentToken->funcName);
        *currentToken = nextToken(lex);

        if (currentToken->type != TOKEN_LPAREN) return 0;
        *currentToken = nextToken(lex);

        // 解析第一个参数
        if (!parseExpression(lex, ce, currentToken)) return 0;

        // 检查是否有第二个参数
        int hasSecondArg = 0;
        if (currentToken->type == TOKEN_COMMA) {
            *currentToken = nextToken(lex);
            if (!parseExpression(lex, ce, currentToken)) return 0;
            hasSecondArg = 1;
        }

        if (currentToken->type != TOKEN_RPAREN) return 0;
        *currentToken = nextToken(lex);

        // 映射函数名到操作码
        if (strcmp(funcName, "sqrt") == 0) emitInstruction(ce, OP_FUNC_SQRT, 0, 0);
        else if (strcmp(funcName, "abs") == 0) emitInstruction(ce, OP_FUNC_ABS, 0, 0);
        else if (strcmp(funcName, "sin") == 0) emitInstruction(ce, OP_FUNC_SIN, 0, 0);
        else if (strcmp(funcName, "cos") == 0) emitInstruction(ce, OP_FUNC_COS, 0, 0);
        else if (strcmp(funcName, "tan") == 0) emitInstruction(ce, OP_FUNC_TAN, 0, 0);
        else if (strcmp(funcName, "log") == 0 || strcmp(funcName, "ln") == 0)
            emitInstruction(ce, OP_FUNC_LOG, 0, 0);
        else if (strcmp(funcName, "log10") == 0) emitInstruction(ce, OP_FUNC_LOG10, 0, 0);
        else if (strcmp(funcName, "exp") == 0) emitInstruction(ce, OP_FUNC_EXP, 0, 0);
        else if (strcmp(funcName, "floor") == 0) emitInstruction(ce, OP_FUNC_FLOOR, 0, 0);
        else if (strcmp(funcName, "ceil") == 0) emitInstruction(ce, OP_FUNC_CEIL, 0, 0);
        else if (strcmp(funcName, "round") == 0) emitInstruction(ce, OP_FUNC_ROUND, 0, 0);
        else if (strcmp(funcName, "min") == 0 && hasSecondArg) emitInstruction(ce, OP_FUNC_MIN, 0, 0);
        else if (strcmp(funcName, "max") == 0 && hasSecondArg) emitInstruction(ce, OP_FUNC_MAX, 0, 0);
        else if (strcmp(funcName, "pow") == 0 && hasSecondArg) emitInstruction(ce, OP_FUNC_POW, 0, 0);
        else return 0;

        return 1;
    }

    if (currentToken->type == TOKEN_LPAREN) {
        *currentToken = nextToken(lex);
        if (!parseExpression(lex, ce, currentToken)) return 0;
        if (currentToken->type != TOKEN_RPAREN) return 0;
        *currentToken = nextToken(lex);
        return 1;
    }

    return 0;
}

// ==================== 编译表达式 ====================

CompiledExpression* compileExpression(const char* expression) {
    CompiledExpression* ce = (CompiledExpression*)CPLMalloc(sizeof(CompiledExpression));
    initCompiledExpr(ce);

    Lexer lex;
    lexerInit(&lex, expression);

    Token token = nextToken(&lex);
    if (!parseExpression(&lex, ce, &token) || token.type != TOKEN_END) {
        freeCompiledExprInternal(ce);
        CPLFree(ce);
        return NULL;
    }

    return ce;
}

void freeCompiledExpression(CompiledExpression* ce) {
    if (ce) {
        freeCompiledExprInternal(ce);
        CPLFree(ce);
    }
}

// ==================== 执行引擎（栈式虚拟机） ====================

#define MAX_STACK_SIZE 256

static inline double executeCompiledExpr(const CompiledExpression* ce,
                                         double** bandData,
                                         int pixelIndex) {
    double stack[MAX_STACK_SIZE];
    int sp = 0;

    for (int i = 0; i < ce->count; i++) {
        const Instruction* inst = &ce->instructions[i];

        switch (inst->op) {
            case OP_LOAD_CONST:
                stack[sp++] = inst->constValue;
                break;

            case OP_LOAD_BAND:
                stack[sp++] = bandData[inst->bandIndex][pixelIndex];
                break;

            case OP_ADD:
                sp--;
                stack[sp-1] = stack[sp-1] + stack[sp];
                break;

            case OP_SUB:
                sp--;
                stack[sp-1] = stack[sp-1] - stack[sp];
                break;

            case OP_MUL:
                sp--;
                stack[sp-1] = stack[sp-1] * stack[sp];
                break;

            case OP_DIV:
                sp--;
                stack[sp-1] = (stack[sp] != 0) ? stack[sp-1] / stack[sp] : NAN;
                break;

            case OP_POW:
                sp--;
                stack[sp-1] = pow(stack[sp-1], stack[sp]);
                break;

            case OP_NEG:
                stack[sp-1] = -stack[sp-1];
                break;

            case OP_FUNC_SQRT:
                stack[sp-1] = sqrt(stack[sp-1]);
                break;

            case OP_FUNC_ABS:
                stack[sp-1] = fabs(stack[sp-1]);
                break;

            case OP_FUNC_SIN:
                stack[sp-1] = sin(stack[sp-1]);
                break;

            case OP_FUNC_COS:
                stack[sp-1] = cos(stack[sp-1]);
                break;

            case OP_FUNC_TAN:
                stack[sp-1] = tan(stack[sp-1]);
                break;

            case OP_FUNC_LOG:
                stack[sp-1] = log(stack[sp-1]);
                break;

            case OP_FUNC_LOG10:
                stack[sp-1] = log10(stack[sp-1]);
                break;

            case OP_FUNC_EXP:
                stack[sp-1] = exp(stack[sp-1]);
                break;

            case OP_FUNC_FLOOR:
                stack[sp-1] = floor(stack[sp-1]);
                break;

            case OP_FUNC_CEIL:
                stack[sp-1] = ceil(stack[sp-1]);
                break;

            case OP_FUNC_ROUND:
                stack[sp-1] = round(stack[sp-1]);
                break;

            case OP_FUNC_MIN:
                sp--;
                stack[sp-1] = fmin(stack[sp-1], stack[sp]);
                break;

            case OP_FUNC_MAX:
                sp--;
                stack[sp-1] = fmax(stack[sp-1], stack[sp]);
                break;

            case OP_FUNC_POW:
                sp--;
                stack[sp-1] = pow(stack[sp-1], stack[sp]);
                break;

            case OP_CMP_GT:
                sp--;
                stack[sp-1] = (stack[sp-1] > stack[sp]) ? 1.0 : 0.0;
                break;

            case OP_CMP_GE:
                sp--;
                stack[sp-1] = (stack[sp-1] >= stack[sp]) ? 1.0 : 0.0;
                break;

            case OP_CMP_LT:
                sp--;
                stack[sp-1] = (stack[sp-1] < stack[sp]) ? 1.0 : 0.0;
                break;

            case OP_CMP_LE:
                sp--;
                stack[sp-1] = (stack[sp-1] <= stack[sp]) ? 1.0 : 0.0;
                break;

            case OP_CMP_EQ:
                sp--;
                stack[sp-1] = (stack[sp-1] == stack[sp]) ? 1.0 : 0.0;
                break;

            case OP_CMP_NE:
                sp--;
                stack[sp-1] = (stack[sp-1] != stack[sp]) ? 1.0 : 0.0;
                break;

            case OP_LOGIC_AND:
                sp--;
                stack[sp-1] = (stack[sp-1] != 0 && stack[sp] != 0) ? 1.0 : 0.0;
                break;

            case OP_LOGIC_OR:
                sp--;
                stack[sp-1] = (stack[sp-1] != 0 || stack[sp] != 0) ? 1.0 : 0.0;
                break;

            default:
                return NAN;
        }
    }

    return (sp > 0) ? stack[sp-1] : NAN;
}

// ==================== 批量计算（分块处理 + OpenMP并行） ====================

#ifdef _OPENMP
#include <omp.h>
#endif

#define BLOCK_SIZE 65536
// osgeo_band_calc.c - 修复 bandData 数组分配

double* calculateBandExpression(GDALDatasetH hDS, const char* expression, int* outSize) {
    if (hDS == NULL || expression == NULL || outSize == NULL) {
        return NULL;
    }

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    size_t totalPixels = (size_t)width * height;

    *outSize = (int)totalPixels;

    CompiledExpression* ce = compileExpression(expression);
    if (ce == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to compile expression: %s", expression);
        return NULL;
    }

    // ★★★ 关键修复：验证波段索引 ★★★
    for (int i = 0; i < ce->usedBandCount; i++) {
        if (ce->usedBands[i] < 1 || ce->usedBands[i] > bandCount) {
            CPLError(CE_Failure, CPLE_AppDefined, "Invalid band index: %d (valid: 1-%d)",
                     ce->usedBands[i], bandCount);
            freeCompiledExpression(ce);
            return NULL;
        }
    }

    // ★★★ 关键修复：分配足够大的数组 ★★★
    // bandCount + 1 是因为波段索引从1开始
    int maxBandIdx = 0;
    for (int i = 0; i < ce->usedBandCount; i++) {
        if (ce->usedBands[i] > maxBandIdx) {
            maxBandIdx = ce->usedBands[i];
        }
    }

    double** bandData = (double**)CPLCalloc(maxBandIdx + 1, sizeof(double*));
    if (bandData == NULL) {
        freeCompiledExpression(ce);
        return NULL;
    }

    // 只加载需要的波段
    for (int i = 0; i < ce->usedBandCount; i++) {
        int bandIdx = ce->usedBands[i];
        bandData[bandIdx] = (double*)CPLMalloc(totalPixels * sizeof(double));
        if (bandData[bandIdx] == NULL) {
            // 清理已分配的内存
            for (int j = 0; j <= maxBandIdx; j++) {
                if (bandData[j]) CPLFree(bandData[j]);
            }
            CPLFree(bandData);
            freeCompiledExpression(ce);
            return NULL;
        }

        GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIdx);
        if (hBand == NULL) {
            for (int j = 0; j <= maxBandIdx; j++) {
                if (bandData[j]) CPLFree(bandData[j]);
            }
            CPLFree(bandData);
            freeCompiledExpression(ce);
            return NULL;
        }

        CPLErr err = GDALRasterIO(hBand, GF_Read, 0, 0, width, height,
                                   bandData[bandIdx], width, height, GDT_Float64, 0, 0);
        if (err != CE_None) {
            for (int j = 0; j <= maxBandIdx; j++) {
                if (bandData[j]) CPLFree(bandData[j]);
            }
            CPLFree(bandData);
            freeCompiledExpression(ce);
            return NULL;
        }
    }

    double* result = (double*)CPLMalloc(totalPixels * sizeof(double));
    if (result == NULL) {
        for (int j = 0; j <= maxBandIdx; j++) {
            if (bandData[j]) CPLFree(bandData[j]);
        }
        CPLFree(bandData);
        freeCompiledExpression(ce);
        return NULL;
    }

#ifdef _OPENMP
    #pragma omp parallel for schedule(static)
#endif
    for (size_t i = 0; i < totalPixels; i++) {
        result[i] = executeCompiledExpr(ce, bandData, (int)i);
    }

    // 清理
    for (int j = 0; j <= maxBandIdx; j++) {
        if (bandData[j]) CPLFree(bandData[j]);
    }
    CPLFree(bandData);
    freeCompiledExpression(ce);

    return result;
}

// ==================== 带条件的计算 ====================
double* calculateBandExpressionWithCondition(GDALDatasetH hDS,
                                              const char* expression,
                                              const char* condition,
                                              double noDataValue,
                                              int* outSize) {
    if (hDS == NULL || expression == NULL || outSize == NULL) {
        return NULL;
    }
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    size_t totalPixels = (size_t)width * height;
    *outSize = (int)totalPixels;
    CompiledExpression* ceExpr = compileExpression(expression);
    if (ceExpr == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to compile expression: %s", expression);
        return NULL;
    }
    CompiledExpression* ceCond = NULL;
    if (condition != NULL && strlen(condition) > 0) {
        ceCond = compileExpression(condition);
        if (ceCond == NULL) {
            CPLError(CE_Failure, CPLE_AppDefined, "Failed to compile condition: %s", condition);
            freeCompiledExpression(ceExpr);
            return NULL;
        }
    }
    int* allBands = (int*)CPLMalloc(sizeof(int) * 64);
    int allBandCount = 0;
    for (int i = 0; i < ceExpr->usedBandCount; i++) {
        int found = 0;
        for (int j = 0; j < allBandCount; j++) {
            if (allBands[j] == ceExpr->usedBands[i]) { found = 1; break; }
        }
        if (!found) allBands[allBandCount++] = ceExpr->usedBands[i];
    }
    if (ceCond) {
        for (int i = 0; i < ceCond->usedBandCount; i++) {
            int found = 0;
            for (int j = 0; j < allBandCount; j++) {
                if (allBands[j] == ceCond->usedBands[i]) { found = 1; break; }
            }
            if (!found) allBands[allBandCount++] = ceCond->usedBands[i];
        }
    }
    for (int i = 0; i < allBandCount; i++) {
        if (allBands[i] < 1 || allBands[i] > bandCount) {
            CPLError(CE_Failure, CPLE_AppDefined, "Invalid band index: %d", allBands[i]);
            CPLFree(allBands);
            freeCompiledExpression(ceExpr);
            if (ceCond) freeCompiledExpression(ceCond);
            return NULL;
        }
    }
    double** bandData = (double**)CPLCalloc(bandCount + 1, sizeof(double*));
    for (int i = 0; i < allBandCount; i++) {
        int bandIdx = allBands[i];
        bandData[bandIdx] = (double*)CPLMalloc(totalPixels * sizeof(double));
        GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIdx);
        CPLErr err = GDALRasterIO(hBand, GF_Read, 0, 0, width, height,
                                   bandData[bandIdx], width, height, GDT_Float64, 0, 0);
        if (err != CE_None) {
            for (int j = 0; j <= bandCount; j++) {
                if (bandData[j]) CPLFree(bandData[j]);
            }
            CPLFree(bandData);
            CPLFree(allBands);
            freeCompiledExpression(ceExpr);
            if (ceCond) freeCompiledExpression(ceCond);
            return NULL;
        }
    }
    CPLFree(allBands);
    double* result = (double*)CPLMalloc(totalPixels * sizeof(double));
#ifdef _OPENMP
    #pragma omp parallel for schedule(static)
#endif
    for (size_t i = 0; i < totalPixels; i++) {
        if (ceCond) {
            double condResult = executeCompiledExpr(ceCond, bandData, (int)i);
            if (condResult == 0.0 || isnan(condResult)) {
                result[i] = noDataValue;
                continue;
            }
        }
        result[i] = executeCompiledExpr(ceExpr, bandData, (int)i);
    }
    for (int j = 0; j <= bandCount; j++) {
        if (bandData[j]) CPLFree(bandData[j]);
    }
    CPLFree(bandData);
    freeCompiledExpression(ceExpr);
    if (ceCond) freeCompiledExpression(ceCond);
    return result;
}
// ==================== 条件替换 ====================
double* conditionalReplace(GDALDatasetH hDS, int bandIndex,
                           double* minValues, double* maxValues,
                           double* newValues, int* includeMin, int* includeMax,
                           int conditionCount, int* outSize) {
    if (hDS == NULL || outSize == NULL || conditionCount <= 0) {
        return NULL;
    }
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    size_t totalPixels = (size_t)width * height;
    *outSize = (int)totalPixels;
    if (bandIndex < 1 || bandIndex > bandCount) {
        return NULL;
    }
    double* bandData = (double*)CPLMalloc(totalPixels * sizeof(double));
    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    CPLErr err = GDALRasterIO(hBand, GF_Read, 0, 0, width, height,
                               bandData, width, height, GDT_Float64, 0, 0);
    if (err != CE_None) {
        CPLFree(bandData);
        return NULL;
    }
    double* result = (double*)CPLMalloc(totalPixels * sizeof(double));
    memcpy(result, bandData, totalPixels * sizeof(double));
#ifdef _OPENMP
    #pragma omp parallel for schedule(static)
#endif
    for (size_t i = 0; i < totalPixels; i++) {
        double v = bandData[i];
        for (int c = 0; c < conditionCount; c++) {
            int minOK = includeMin[c] ? (v >= minValues[c]) : (v > minValues[c]);
            int maxOK = includeMax[c] ? (v <= maxValues[c]) : (v < maxValues[c]);
            if (minOK && maxOK) {
                result[i] = newValues[c];
                break;
            }
        }
    }
    CPLFree(bandData);
    return result;
}
// ==================== 分块计算器（用于超大影像） ====================
// ★★★ 关键修改：使用命名结构体 ★★★
struct BlockCalculator {
    GDALDatasetH hDS;
    CompiledExpression* ce;
    int blockWidth;
    int blockHeight;
    int numBlocksX;
    int numBlocksY;
};
BlockCalculator* createBlockCalculator(GDALDatasetH hDS, const char* expression,
                                        int blockWidth, int blockHeight) {
    if (hDS == NULL || expression == NULL) return NULL;
    BlockCalculator* bc = (BlockCalculator*)CPLMalloc(sizeof(BlockCalculator));
    bc->hDS = hDS;
    bc->ce = compileExpression(expression);
    if (bc->ce == NULL) {
        CPLFree(bc);
        return NULL;
    }
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    bc->blockWidth = blockWidth;
    bc->blockHeight = blockHeight;
    bc->numBlocksX = (width + blockWidth - 1) / blockWidth;
    bc->numBlocksY = (height + blockHeight - 1) / blockHeight;
    return bc;
}
void freeBlockCalculator(BlockCalculator* bc) {
    if (bc) {
        if (bc->ce) freeCompiledExpression(bc->ce);
        CPLFree(bc);
    }
}
double* calculateBlock(BlockCalculator* bc, int blockX, int blockY,
                       int* outWidth, int* outHeight) {
    if (bc == NULL || outWidth == NULL || outHeight == NULL) return NULL;
    int imgWidth = GDALGetRasterXSize(bc->hDS);
    int imgHeight = GDALGetRasterYSize(bc->hDS);
    int bandCount = GDALGetRasterCount(bc->hDS);
    int xOff = blockX * bc->blockWidth;
    int yOff = blockY * bc->blockHeight;
    int actualWidth = bc->blockWidth;
    int actualHeight = bc->blockHeight;
    if (xOff + actualWidth > imgWidth) actualWidth = imgWidth - xOff;
    if (yOff + actualHeight > imgHeight) actualHeight = imgHeight - yOff;
    *outWidth = actualWidth;
    *outHeight = actualHeight;
    size_t blockPixels = (size_t)actualWidth * actualHeight;
    double** bandData = (double**)CPLCalloc(bandCount + 1, sizeof(double*));
    for (int i = 0; i < bc->ce->usedBandCount; i++) {
        int bandIdx = bc->ce->usedBands[i];
        bandData[bandIdx] = (double*)CPLMalloc(blockPixels * sizeof(double));
        GDALRasterBandH hBand = GDALGetRasterBand(bc->hDS, bandIdx);
        CPLErr err = GDALRasterIO(hBand, GF_Read, xOff, yOff, actualWidth, actualHeight,
                                   bandData[bandIdx], actualWidth, actualHeight,
                                   GDT_Float64, 0, 0);
        if (err != CE_None) {
            for (int j = 0; j <= bandCount; j++) {
                if (bandData[j]) CPLFree(bandData[j]);
            }
            CPLFree(bandData);
            return NULL;
        }
    }
    double* result = (double*)CPLMalloc(blockPixels * sizeof(double));
#ifdef _OPENMP
    #pragma omp parallel for schedule(static)
#endif
    for (size_t i = 0; i < blockPixels; i++) {
        result[i] = executeCompiledExpr(bc->ce, bandData, (int)i);
    }
    for (int j = 0; j <= bandCount; j++) {
        if (bandData[j]) CPLFree(bandData[j]);
    }
    CPLFree(bandData);
    return result;
}
// ==================== 预定义指数计算（高度优化） ====================
double* calculateNDVI(GDALDatasetH hDS, int nirBand, int redBand, int* outSize) {
    if (hDS == NULL || outSize == NULL) return NULL;
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    size_t totalPixels = (size_t)width * height;
    *outSize = (int)totalPixels;
    if (nirBand < 1 || nirBand > bandCount || redBand < 1 || redBand > bandCount) {
        return NULL;
    }
    double* nirData = (double*)CPLMalloc(totalPixels * sizeof(double));
    double* redData = (double*)CPLMalloc(totalPixels * sizeof(double));
    GDALRasterIO(GDALGetRasterBand(hDS, nirBand), GF_Read, 0, 0, width, height,
                 nirData, width, height, GDT_Float64, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, redBand), GF_Read, 0, 0, width, height,
                 redData, width, height, GDT_Float64, 0, 0);
    double* result = (double*)CPLMalloc(totalPixels * sizeof(double));
#ifdef _OPENMP
    #pragma omp parallel for schedule(static)
#endif
    for (size_t i = 0; i < totalPixels; i++) {
        double nir = nirData[i];
        double red = redData[i];
        double sum = nir + red;
        result[i] = (sum != 0) ? (nir - red) / sum : NAN;
    }
    CPLFree(nirData);
    CPLFree(redData);
    return result;
}
double* calculateNDWI(GDALDatasetH hDS, int greenBand, int nirBand, int* outSize) {
    if (hDS == NULL || outSize == NULL) return NULL;
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    size_t totalPixels = (size_t)width * height;
    *outSize = (int)totalPixels;
    if (greenBand < 1 || greenBand > bandCount || nirBand < 1 || nirBand > bandCount) {
        return NULL;
    }
    double* greenData = (double*)CPLMalloc(totalPixels * sizeof(double));
    double* nirData = (double*)CPLMalloc(totalPixels * sizeof(double));
    GDALRasterIO(GDALGetRasterBand(hDS, greenBand), GF_Read, 0, 0, width, height,
                 greenData, width, height, GDT_Float64, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, nirBand), GF_Read, 0, 0, width, height,
                 nirData, width, height, GDT_Float64, 0, 0);
    double* result = (double*)CPLMalloc(totalPixels * sizeof(double));
#ifdef _OPENMP
    #pragma omp parallel for schedule(static)
#endif
    for (size_t i = 0; i < totalPixels; i++) {
        double green = greenData[i];
        double nir = nirData[i];
        double sum = green + nir;
        result[i] = (sum != 0) ? (green - nir) / sum : NAN;
    }
    CPLFree(greenData);
    CPLFree(nirData);
    return result;
}
double* calculateEVI(GDALDatasetH hDS, int nirBand, int redBand, int blueBand, int* outSize) {
    if (hDS == NULL || outSize == NULL) return NULL;
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    size_t totalPixels = (size_t)width * height;
    *outSize = (int)totalPixels;
    if (nirBand < 1 || nirBand > bandCount ||
        redBand < 1 || redBand > bandCount ||
        blueBand < 1 || blueBand > bandCount) {
        return NULL;
    }
    double* nirData = (double*)CPLMalloc(totalPixels * sizeof(double));
    double* redData = (double*)CPLMalloc(totalPixels * sizeof(double));
    double* blueData = (double*)CPLMalloc(totalPixels * sizeof(double));
    GDALRasterIO(GDALGetRasterBand(hDS, nirBand), GF_Read, 0, 0, width, height,
                 nirData, width, height, GDT_Float64, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, redBand), GF_Read, 0, 0, width, height,
                 redData, width, height, GDT_Float64, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, blueBand), GF_Read, 0, 0, width, height,
                 blueData, width, height, GDT_Float64, 0, 0);
    double* result = (double*)CPLMalloc(totalPixels * sizeof(double));
#ifdef _OPENMP
    #pragma omp parallel for schedule(static)
#endif
    for (size_t i = 0; i < totalPixels; i++) {
        double nir = nirData[i];
        double red = redData[i];
        double blue = blueData[i];
        double denom = nir + 6.0 * red - 7.5 * blue + 1.0;
        result[i] = (denom != 0) ? 2.5 * (nir - red) / denom : NAN;
    }
    CPLFree(nirData);
    CPLFree(redData);
    CPLFree(blueData);
    return result;
}

void freeBandCalcResult(double* ptr) {
    if (ptr) CPLFree(ptr);
}
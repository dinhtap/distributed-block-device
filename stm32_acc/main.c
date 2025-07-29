#include <stm32.h>
#include <gpio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>

#define USART_Mode_Rx_Tx (USART_CR1_RE | \
                            USART_CR1_TE)
#define USART_Enable USART_CR1_UE

#define USART_WordLength_8b 0x0000
#define USART_WordLength_9b USART_CR1_M

#define USART_Parity_No 0x0000
#define USART_Parity_Even USART_CR1_PCE
#define USART_Parity_Odd (USART_CR1_PCE | \
                            USART_CR1_PS)

#define USART_StopBits_1 0x0000
#define USART_StopBits_0_5 0x1000
#define USART_StopBits_2 0x2000
#define USART_StopBits_1_5 0x3000

#define USART_FlowControl_None 0x0000
#define USART_FlowControl_RTS USART_CR3_RTSE
#define USART_FlowControl_CTS USART_CR3_CTSE

#define HSI_HZ 16000000U
#define PCLK1_HZ HSI_HZ
#define BAUD 9600U

#define TIMER_ARR 64000
#define TIMER_PSC 5

#define FIRST_START 0
#define SECOND_START 1

#define LIS35DE_ADDR 0x1C
#define LIS35DE_CTRL_REG_1 0x20
#define LIS35DE_OUT_X 0x29
#define LIS35DE_OUT_Y 0x2B

#define BUFSIZE 5000
#define BUFENTRYSIZE 8
typedef uint32_t bufsizetype;

typedef struct entry {
    char content[BUFENTRYSIZE];
    bufsizetype count;
} entry;

typedef struct cyclicbuf {
    entry entries[BUFSIZE];
    bufsizetype buf_it;
    bufsizetype count;
} cyclicbuf;

// entry consuming_entry = {
//     .content = {0},
//     .count = 0
// };

void pushbuf(cyclicbuf* buf, char* to_push_buf, uint8_t count) {
    if (buf->count < BUFSIZE) {
        buf->entries[buf->buf_it].count = count;
        for (int i = 0; i < count; ++i) {
            buf->entries[buf->buf_it].content[i] = to_push_buf[i];
        }
        buf->buf_it = (buf->buf_it + 1) % BUFSIZE;
        ++(buf->count);
    }
}

char is_empty(cyclicbuf* buf) {
    return buf->count == 0;
}

int get_it_to_consume(cyclicbuf* buf) {
    bufsizetype consume_it;
    if (buf->buf_it >= buf->count) {
        consume_it = buf->buf_it - buf->count;
    }
    else {
        consume_it = BUFSIZE - buf->count + buf->buf_it;
    }

    return consume_it;
}

cyclicbuf buf = {
    .buf_it = 0,
    .count = 0
};


typedef struct read_accelerometer_step {
    int this_start;
    int dimension;
} read_accelerometer_step;

read_accelerometer_step read_acc_step = {
    .this_start = FIRST_START,
    .dimension = LIS35DE_OUT_X
};

const char accelerometer_config = 0b01000111;
int inited_accelerator = 0;
// End global states.

void send_dma() {
    bufsizetype it = get_it_to_consume(&buf);
    DMA1_Stream6->M0AR = (uint32_t) buf.entries[it].content;
    DMA1_Stream6->NDTR = buf.entries[it].count;
    DMA1_Stream6->CR |= DMA_SxCR_EN;
}

void push_and_init_send(char* str, uint8_t count) {
    pushbuf(&buf, str, count);

    if ((DMA1_Stream6->CR & DMA_SxCR_EN) == 0 && (DMA1->HISR & DMA_HISR_TCIF6) == 0) {
        send_dma();
    }
}

void DMA1_Stream6_IRQHandler() {
    /* Odczytaj zgłoszone przerwania DMA1. */
    uint32_t isr = DMA1->HISR;
    --(buf.count);
    if (isr & DMA_HISR_TCIF6) {
    /* Obsłuż zakończenie transferu
    w strumieniu 6. */
        DMA1->HIFCR = DMA_HIFCR_CTCIF6;
        if (!is_empty(&buf)) {
            send_dma();
        }
    }
}

void dma_configure(void) {

    RCC->AHB1ENR |= RCC_AHB1ENR_GPIOAEN |
                    RCC_AHB1ENR_DMA1EN;
    RCC->APB1ENR |= RCC_APB1ENR_USART2EN;
    __NOP();

    GPIOafConfigure(GPIOA,
                    2,
                    GPIO_OType_PP,
                    GPIO_Fast_Speed,
                    GPIO_PuPd_NOPULL,
                    GPIO_AF_USART2);

    USART2->CR1 = USART_CR1_RE | USART_CR1_TE;
    USART2->CR2 = 0;

    USART2->BRR = (PCLK1_HZ + (BAUD / 2U)) / BAUD;

    USART2->CR3 = USART_CR3_DMAT | USART_CR3_DMAR;

    DMA1_Stream6->CR =  4U << 25 |
                        DMA_SxCR_PL_1 |
                        DMA_SxCR_MINC |
                        DMA_SxCR_DIR_0 |
                        DMA_SxCR_TCIE;
    DMA1_Stream6->PAR = (uint32_t)&USART2->DR;

    DMA1->HIFCR = DMA_HIFCR_CTCIF6;
    NVIC_EnableIRQ(DMA1_Stream6_IRQn);

    USART2->CR1 |= USART_Enable;
}


void configure_timer() {
    RCC->APB1ENR |= RCC_APB1ENR_TIM3EN;
    TIM3->CR1 = TIM_CR1_URS | 0;
    TIM3->PSC = TIMER_PSC;
    TIM3->ARR = TIMER_ARR;
    TIM3->EGR = TIM_EGR_UG;
    TIM3->CR1 |= TIM_CR1_CEN;

    // Przerwanie
    TIM3->SR = ~TIM_SR_UIF;
    TIM3->DIER = TIM_DIER_UIE;
    NVIC_EnableIRQ(TIM3_IRQn);
}

void TIM3_IRQHandler(void) {
    uint32_t it_status = TIM3->SR & TIM3->DIER;
    if (it_status & TIM_SR_UIF) {
        TIM3->SR = ~TIM_SR_UIF;

        if (inited_accelerator) {
            read_acc_step.this_start = FIRST_START;
            read_acc_step.dimension = LIS35DE_OUT_X;

            // Initiate read, first X, then Y (here inits X)
            // Every read has 2 STARTs
            I2C1->CR1 |= I2C_CR1_START;
            I2C1->CR2 |= I2C_CR2_ITBUFEN;
        }

    }
}

#define I2C_SPEED_HZ 100000
#define PCLK1_MHZ 16
void I2C1_ER_IRQHandler(void) {
    I2C1->CR1 |= I2C_CR1_STOP;
}

void I2C1_EV_IRQHandler(void) {
    uint32_t sr1 = I2C1->SR1;
    if (sr1 & I2C_SR1_SB) {
        if (!inited_accelerator) {
            I2C1->DR = LIS35DE_ADDR << 1;
        }

        else if (read_acc_step.this_start == FIRST_START) {
            I2C1->DR = LIS35DE_ADDR << 1;
        }
        else {
            I2C1->DR = LIS35DE_ADDR << 1 | 1;
            I2C1->CR1 &= ~I2C_CR1_ACK;
        }
    }
    
    if (sr1 & I2C_SR1_ADDR) {
        I2C1->SR2;
        if (!inited_accelerator) {
            I2C1->DR = LIS35DE_CTRL_REG_1;
        }

        else if (read_acc_step.this_start == FIRST_START) {
            I2C1->DR = read_acc_step.dimension;
        }
        else {
            I2C1->CR1 |= I2C_CR1_STOP;
        }
    }

    if (!inited_accelerator) {
        if (sr1 & I2C_SR1_TXE) {
            I2C1->DR = accelerometer_config;
            I2C1->CR2 &= ~I2C_CR2_ITBUFEN;
        }

        if (sr1 & I2C_SR1_BTF) {
            inited_accelerator = 1;
            I2C1->CR1 |= I2C_CR1_STOP;
            configure_timer();
        }
    }

    if (sr1 & I2C_SR1_BTF && read_acc_step.this_start == FIRST_START) {
        read_acc_step.this_start = SECOND_START;
        I2C1->CR1 |= I2C_CR1_START;
    }

    if (sr1 & I2C_SR1_RXNE) {
        char dim = read_acc_step.dimension == LIS35DE_OUT_X ? 'X' : 'Y';
        signed char value = I2C1->DR;
        char value_str[8] = {'0'};
        value_str[0] = dim;
        int val = value;
        itoa(val, value_str + 1, 10);
        int len = strlen(value_str);
        if (dim == 'Y') {
            value_str[len] = '\r';
            value_str[len + 1] = '\n';
            len += 2;
        }
        push_and_init_send(value_str, len);

        // Switch to Y or finish
        if (read_acc_step.dimension == LIS35DE_OUT_Y) {
            I2C1->CR2 &= ~I2C_CR2_ITBUFEN;
        }
        else {
            read_acc_step.dimension = LIS35DE_OUT_Y;
            read_acc_step.this_start = FIRST_START;
            I2C1->CR1 |= I2C_CR1_START;
        }
    }
}

void i2c_configure() {
    RCC->AHB1ENR |= RCC_AHB1ENR_GPIOBEN;
    RCC->APB1ENR |= RCC_APB1ENR_I2C1EN;

    GPIOafConfigure(GPIOB, 8, GPIO_OType_OD,
                    GPIO_Low_Speed, GPIO_PuPd_NOPULL,
                    GPIO_AF_I2C1);
    GPIOafConfigure(GPIOB, 9, GPIO_OType_OD,
                    GPIO_Low_Speed, GPIO_PuPd_NOPULL,
                    GPIO_AF_I2C1);

    I2C1->CR1 = 0;

    I2C1->CCR = (PCLK1_MHZ * 1000000) / (I2C_SPEED_HZ << 1);
    I2C1->CR2 = PCLK1_MHZ;
    I2C1->TRISE = PCLK1_MHZ + 1;

    // Przerwanie
    I2C1->CR2 |= I2C_CR2_ITEVTEN | I2C_CR2_ITERREN | I2C_CR2_ITBUFEN;
    NVIC_EnableIRQ(I2C1_EV_IRQn);
    NVIC_EnableIRQ(I2C1_ER_IRQn);

    // Wlaczenie 
    I2C1->CR1 |= I2C_CR1_PE;
}

void accelerometer_and_timer_configure() {
    // Just start saving configurations to accelerometer, handled by interrupts
    // Timer configured after accelerometer is ready
    I2C1->CR1 |= I2C_CR1_START;
}

int main() {
    dma_configure();
    i2c_configure();
    accelerometer_and_timer_configure();

    for(;;);
}

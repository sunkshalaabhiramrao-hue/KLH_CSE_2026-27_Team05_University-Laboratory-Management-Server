#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 8080
#define BUFFER_SIZE 1024

int main(void)
{
    int client_socket;

    struct sockaddr_in server_address;

    char request[BUFFER_SIZE];

    char response[BUFFER_SIZE];


    /*
     * Create socket.
     */
    client_socket =
        socket(
            AF_INET,
            SOCK_STREAM,
            0
        );


    if (client_socket == -1)
    {
        perror("socket");

        return 1;
    }


    /*
     * Configure server address.
     */
    memset(
        &server_address,
        0,
        sizeof(server_address)
    );


    server_address.sin_family =
        AF_INET;

    server_address.sin_port =
        htons(PORT);


    /*
     * Connect to localhost.
     */
    if (inet_pton(
            AF_INET,
            "127.0.0.1",
            &server_address.sin_addr
        ) <= 0)
    {
        perror("inet_pton");

        close(client_socket);

        return 1;
    }


    if (connect(
            client_socket,
            (struct sockaddr *)&server_address,
            sizeof(server_address)
        ) == -1)
    {
        perror("connect");

        close(client_socket);

        return 1;
    }


    printf("\n");
    printf("====================================\n");
    printf(" University Laboratory Student Client\n");
    printf("====================================\n");

    printf("\n");
    printf("Available commands:\n");
    printf("\n");

    printf("101 REQUEST COMPUTER\n");
    printf("101 RELEASE COMPUTER\n");

    printf("101 REQUEST PROGRAM\n");
    printf("101 RELEASE PROGRAM\n");

    printf("101 REQUEST FILE\n");
    printf("101 RELEASE FILE\n");

    printf("STATUS\n");

    printf("\n");
    printf("Enter request: ");


    /*
     * Read student's command.
     */
    if (fgets(
            request,
            sizeof(request),
            stdin
        ) == NULL)
    {
        close(client_socket);

        return 1;
    }


    /*
     * Remove newline.
     */
    request[
        strcspn(
            request,
            "\n"
        )
    ] = '\0';


    /*
     * Send request.
     */
    if (send(
            client_socket,
            request,
            strlen(request),
            0
        ) == -1)
    {
        perror("send");

        close(client_socket);

        return 1;
    }


    printf(
        "\nRequest sent: %s\n",
        request
    );


    /*
     * Receive response.
     */
    memset(
        response,
        0,
        sizeof(response)
    );


    ssize_t bytes_received =
        recv(
            client_socket,
            response,
            sizeof(response) - 1,
            0
        );


    if (bytes_received == -1)
    {
        perror("recv");

        close(client_socket);

        return 1;
    }


    printf(
        "Server response: %s",
        response
    );


    close(client_socket);


    return 0;
}


// C code for linked list merged sort
#include <stdio.h>
#include <stdlib.h>
#include <mapreduce.h>
 
/* function prototypes */
node_t* SortedMerge(node_t* a, node_t* b);
void FrontBackSplit(node_t* source,
                    node_t** frontRef, node_t** backRef);
 
/* sorts the linked list by changing next pointers (not key) */
void MergeSort(node_t** headRef)
{
    node_t* head = *headRef;
    node_t* a;
    node_t* b;
 
    /* Base case -- length 0 or 1 */
    if ((head == NULL) || (head->next == NULL)) {
        return;
    }
 
    /* Split head into 'a' and 'b' sublists */
    FrontBackSplit(head, &a, &b);
 
    /* Recursively sort the sublists */
    MergeSort(&a);
    MergeSort(&b);
 
    /* answer = merge the two sorted lists together */
    *headRef = SortedMerge(a, b);
}
 
/* See https:// www.geeksforgeeks.org/?p=3622 for details of this
function */
node_t* SortedMerge(node_t* a, node_t* b)
{
    node_t* result = NULL;
 
    /* Base cases */
    if (a == NULL)
        return (b);
    else if (b == NULL)
        return (a);
 
    /* Pick either a or b, and recur */
    if (a->key <= b->key) {
        result = a;
        result->next = SortedMerge(a->next, b);
    }
    else {
        result = b;
        result->next = SortedMerge(a, b->next);
    }
    return (result);
}
 
/* UTILITY FUNCTIONS */
/* Split the nodes of the given list into front and back halves,
    and return the two lists using the reference parameters.
    If the length is odd, the extra node should go in the front list.
    Uses the fast/slow pointer strategy. */
void FrontBackSplit(node_t* source,
                    node_t** frontRef, node_t** backRef)
{
    node_t* fast;
    node_t* slow;
    slow = source;
    fast = source->next;
 
    /* Advance 'fast' two nodes, and advance 'slow' one node */
    while (fast != NULL) {
        fast = fast->next;
        if (fast != NULL) {
            slow = slow->next;
            fast = fast->next;
        }
    }
 
    /* 'slow' is before the midpoint in the list, so split it in two
    at that point. */
    *frontRef = source;
    *backRef = slow->next;
    slow->next = NULL;
}
 
/* Function to print nodes in a given linked list */
void printList(node_t* node)
{
    while (node != NULL) {
        printf("%d ", node->key);
        node = node->next;
    }
}
 
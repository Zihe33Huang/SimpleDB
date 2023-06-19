package simpledb.storage;

/**
 * @author huangzihe
 * @date 2023/6/15 3:44 PM
 */
public class PageList {

    PageNode dummy;

    PageNode tail;

    int size;


    public PageList() {
        this.dummy = new PageNode(null, null);
        this.tail = dummy;
        this.size = 0;
    }

    class PageNode {
        PageId pageId;
        Page page;
        PageNode prev;

        PageNode next;

        public PageNode(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }
    }

    public PageNode add(Page page) {
        PageNode pageNode = new PageNode(page.getId(), page);
        add(pageNode);
        return pageNode;
    }

    private void add(PageNode pageNode) {
        PageNode p = this.dummy.next;
        while (p != null) {
            if (p.pageId.equals(pageNode.pageId)) {
                moveToHead(p);
                return;
            }
            p = p.next;
        }
        addToHead(pageNode);
    }

    public int getSize() {
        return size;
    }
//
//    public Page get(PageId pageId) {
//        PageNode p = this.dummy.next;
//        while (p != null) {
//            if (p.pageId.equals(pageId)) {
//                this.moveToHead(p);
//                return p.page;
//            }
//            p = p.next;
//        }
//        return null;
//    }


    public void addToHead(PageNode pageNode) {
        PageNode oldHead = dummy.next;
        pageNode.next = oldHead;
        if (oldHead != null) {
            oldHead.prev = pageNode;
        } else {
            tail = pageNode;
        }
        pageNode.prev = dummy;
        dummy.next = pageNode;
        size++;
    }

    private void delNode(PageNode pageNode) {
        pageNode.prev.next = pageNode.next;
        if (pageNode.next != null) {
            pageNode.next.prev = pageNode.prev;
        }
        size--;
    }

    public void moveToHead(PageNode pageNode) {
        delNode(pageNode);
        addToHead(pageNode);
    }

    public PageNode delTail() {
        PageNode oldTail = tail;
        tail = tail.prev;
        delNode(oldTail);
        return oldTail;
    }


}

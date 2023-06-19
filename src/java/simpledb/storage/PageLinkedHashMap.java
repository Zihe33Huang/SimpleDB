package simpledb.storage;

import java.util.HashMap;

/**
 * @author huangzihe
 * @date 2023/6/16 6:19 PM
 */
public class PageLinkedHashMap {
    PageList list;

    HashMap<PageId, PageList.PageNode> hashMap;

    int max;


    public PageLinkedHashMap(int max) {
        this.max = max;
        this.list = new PageList();
        this.hashMap = new HashMap<>();
    }

    public void put(Page page) {
        if (this.list.getSize() >= max) {
            PageList.PageNode pageNode = this.list.delTail();
            this.hashMap.remove(pageNode.pageId);
        }
        PageList.PageNode add = this.list.add(page);
        this.hashMap.put(page.getId(), add);
    }

    public Page get(PageId pageId) {
        PageList.PageNode pageNode = this.hashMap.get(pageId);
        this.list.moveToHead(pageNode);
        return pageNode.page;
    }


}

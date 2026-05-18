import { DeleteOutlined, EditOutlined, PushpinOutlined } from "@ant-design/icons";
import { Button, Empty, Form, Input, Modal, Popconfirm, Tag } from "antd";
import { useEffect, useState } from "react";
import { shortAddress } from "../utils/addressBook";

const EMPTY_DRAFT = {
  id: "",
  name: "",
  address: "",
  makeDefault: false,
};

export default function AddressBookModal({
  open,
  onClose,
  entries,
  currentAddress,
  draftSeed,
  onSubmit,
  onApply,
  onDelete,
  onSetDefault,
}) {
  const [draft, setDraft] = useState(EMPTY_DRAFT);

  useEffect(() => {
    if (!open) {
      return;
    }
    setDraft(
      draftSeed
        ? {
            id: draftSeed.id || "",
            name: draftSeed.name || "",
            address: draftSeed.address || "",
            makeDefault: Boolean(draftSeed.makeDefault),
          }
        : EMPTY_DRAFT,
    );
  }, [draftSeed, open]);

  async function handleSubmit() {
    const shouldReset = await onSubmit(draft);
    if (shouldReset !== false) {
      setDraft(EMPTY_DRAFT);
    }
  }

  function handleEdit(entry) {
    setDraft({
      id: entry.id,
      name: entry.name,
      address: entry.address,
      makeDefault: false,
    });
  }

  return (
    <Modal
      title="Address Book"
      open={open}
      onCancel={onClose}
      footer={<Button onClick={onClose}>Done</Button>}
      destroyOnHidden
      width={760}
    >
      <div className="address-book-caption">Save frequently used addresses with a name, set a default, and reuse them from the main panel.</div>

      <Form layout="vertical" requiredMark={false} className="address-book-form">
        <div className="address-book-form-grid">
          <Form.Item label="Name">
            <Input
              value={draft.name}
              onChange={(event) => setDraft((prev) => ({ ...prev, name: event.target.value }))}
              placeholder="Trader Alpha"
            />
          </Form.Item>

          <Form.Item label="Address">
            <Input
              value={draft.address}
              onChange={(event) => setDraft((prev) => ({ ...prev, address: event.target.value }))}
              placeholder="0x..."
            />
          </Form.Item>
        </div>

        <div className="address-book-form-actions">
          <Button type="primary" onClick={handleSubmit}>
            {draft.id ? "Update Address" : "Save Address"}
          </Button>
          {draft.id ? <Button onClick={() => setDraft(EMPTY_DRAFT)}>Cancel Edit</Button> : null}
        </div>
      </Form>

      <div className="address-book-list">
        {entries.length ? (
          entries.map((entry) => {
            const isCurrent = entry.address === currentAddress;
            return (
              <div key={entry.id} className={`address-book-item ${isCurrent ? "active" : ""}`}>
                <div className="address-book-item-main">
                  <div className="address-book-item-header">
                    <span className="address-book-item-name">{entry.name}</span>
                    {entry.isDefault ? (
                      <Tag color="gold" bordered={false}>
                        Default
                      </Tag>
                    ) : null}
                    {isCurrent ? (
                      <Tag color="cyan" bordered={false}>
                        Current
                      </Tag>
                    ) : null}
                  </div>
                  <div className="address-book-item-address" title={entry.address}>
                    {entry.address}
                  </div>
                  <div className="address-book-item-footnote">{shortAddress(entry.address)}</div>
                </div>

                <div className="address-book-item-actions">
                  <Button size="small" type={isCurrent ? "primary" : "default"} onClick={() => onApply(entry)}>
                    Use
                  </Button>
                  <Button
                    size="small"
                    icon={<PushpinOutlined />}
                    onClick={() => onSetDefault(entry.id)}
                    disabled={entry.isDefault}
                  >
                    Default
                  </Button>
                  <Button size="small" icon={<EditOutlined />} onClick={() => handleEdit(entry)}>
                    Edit
                  </Button>
                  <Popconfirm title="Delete this saved address?" onConfirm={() => onDelete(entry.id)} okText="Delete" cancelText="Cancel">
                    <Button size="small" danger icon={<DeleteOutlined />}>
                      Delete
                    </Button>
                  </Popconfirm>
                </div>
              </div>
            );
          })
        ) : (
          <Empty description="No saved addresses yet" />
        )}
      </div>
    </Modal>
  );
}

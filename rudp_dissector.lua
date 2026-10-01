local proto=proto("rudp","RUDP Protocol")

local fields = {
    seq = ProtoField.uint32("rudp.seq", "Sequence Number", base.DEC),
    ack = ProtoField.uint32("rudp.ack", "Acknowledgment Number", base.DEC),
    flags = ProtoField.uint16("rudp.flags", "Flags", base.HEX),
    length = ProtoField.uint16("rudp.length", "Length", base.DEC),
    checksum = ProtoField.uint16("rudp.checksum", "Checksum", base.HEX),
    src_id = ProtoField.uint32("rudp.src_id", "Source Node ID", base.DEC),
    dst_id = ProtoField.uint32("rudp.dst_id", "Destination Node ID", base.DEC)
}

proto.fields = fields

function proto.dissector(buffer, pinfo, tree)
    pinfo.cols.protocol = "RUDP"
    local subtree = tree:add(proto, buffer(), "RUDP Protocol Data")
    local offset = 0
    subtree:add(fields.seq, buffer(offset, 4))
    offset = offset + 4
    subtree:add(fields.ack, buffer(offset, 4))
    offset = offset + 4
    subtree:add(fields.flags, buffer(offset, 2))
    offset = offset + 2
    subtree:add(fields.length, buffer(offset, 2))
    offset = offset + 2
    subtree:add(fields.checksum, buffer(offset, 2))
    offset = offset + 2
    subtree:add(fields.src_id, buffer(offset, 4))
    offset = offset + 4
    subtree:add(fields.dst_id, buffer(offset, 4))
    offset = offset + 4
    local len = buffer(10, 2):uint()
    if len > 0 then
        subtree:add(buffer(offset, len), "Payload")
    end
end

local udp_port = DissectorTable.get("udp.port")
udp_port:add(8888, proto)